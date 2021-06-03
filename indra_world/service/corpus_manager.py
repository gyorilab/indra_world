"""This module allows running one-off assembly on a set of DART records
(i.e., reader outputs) into a 'seed corpus' that can be dumped on S3
for loading into CauseMos."""
import os
import json
import tqdm
import logging
import datetime
from indra.statements import stmts_to_json, stmts_to_json_file
from indra_world import default_bucket, default_key_base
from indra_world.assembly.incremental_assembler import IncrementalAssembler
from .controller import ServiceController


logger = logging.getLogger(__name__)


class CorpusManager:
    """Corpus manager class allowing running assembly on a set of DART records.
    """
    def __init__(self, db_url, dart_records, corpus_id, metadata,
                 dart_client=None):
        self.sc = ServiceController(db_url=db_url, dart_client=dart_client)
        self.corpus_id = corpus_id
        self.dart_records = dart_records
        self.metadata = metadata
        self.assembled_stmts = None

    def prepare(self, records_exist=False):
        """Run the preprocessing pipeline on statements.

        This function adds the new corpus to the DB, adds records to the
        new corpus, then processes the reader outputs for those records into
        statements, preprocesses the statements, and then stores these
        prepared statements in the DB.
        """
        logger.info('Adding corpus %s to DB' % self.corpus_id)
        self.sc.db.add_corpus(self.corpus_id, self.metadata)
        logger.info('Adding %d records for corpus' % len(self.dart_records))
        self.sc.db.add_records_for_corpus(
            self.corpus_id,
            [c['storage_key'] for c in self.dart_records]
        )
        if not records_exist:
            logger.info('Adding and processing records')
            for record in tqdm.tqdm(self.dart_records):
                # This adds DART records
                self.sc.add_dart_record(record)
                # This adds prepared statements
                self.sc.process_dart_record(record)

    def assemble(self):
        """Run assembly on the prepared statements.

        This function loads all the prepared statements associated with the
        corpus and then runs assembly on them.
        """
        all_stmts = []
        logger.info('Loading statements from DB for %d records' %
                    len(self.dart_records))
        for record in tqdm.tqdm(self.dart_records):
            stmts = self.sc.db.get_statements_for_record(record['storage_key'])
            all_stmts += stmts
        logger.info('Instantiating incremental assembler with %d statements'
                    % len(all_stmts))
        ia = IncrementalAssembler(all_stmts)
        logger.info('Getting assembled statements')
        self.assembled_stmts = ia.get_statements()
        logger.info('Got %d assembled statements' % len(self.assembled_stmts))
        self.metadata['num_statements'] = len(self.assembled_stmts)

    def dump_local(self, base_folder):
        """Dump assembled corpus into local files."""
        corpus_folder = os.path.join(base_folder, self.corpus_id)
        os.makedirs(corpus_folder, exist_ok=True)
        stmts_to_json_file(self.assembled_stmts,
                           os.path.join(corpus_folder, 'statements.json'),
                           format='jsonl')
        with open(os.path.join(corpus_folder, 'metadata.json'), 'w') as fh:
            json.dump(fh, self.metadata)

    def dump_s3(self):
        """Dump assembled corpus onto S3."""
        logger.info('Uploading %s to S3' % self.corpus_id)
        s3 = _make_s3_client()

        # Upload statements
        jsonl_str = stmts_to_jsonl_str(self.assembled_stmts)
        key = os.path.join(default_key_base, self.corpus_id, 'statements.json')
        s3.put_object(Body=jsonl_str, Bucket=default_bucket, Key=key)

        # Upload meta data
        metadata_str = json.dumps(self.metadata, indent=1)
        key = os.path.join(default_key_base, self.corpus_id, 'metadata.json')
        s3.put_object(Body=metadata_str, Bucket=default_bucket, Key=key)

        # Update index
        key = os.path.join(default_key_base, self.corpus_id, 'index.csv')
        obj = s3.get_object(Bucket=default_bucket, Key=key)
        index_str = obj['Body'].read().decode('utf-8')
        if not index_str.endswith('\n'):
            index_str += '\n'
        index_str += '%s,%s\n' % (
            self.corpus_id,
            datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S'))
        index_bytes = index_str.encode('utf-8')
        s3.put_object(Bucket=default_bucket, Key=key, Body=index_bytes)

    def _get_doc_ids_from_records(self):
        return sorted({record['document_id'] for record in self.dart_records})


def _make_s3_client(profile_name='wm'):
    import boto3
    key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if key_id and secret_key:
        logger.info('Got credentials in environment for client')
        sess = boto3.session.Session(aws_access_key_id=key_id,
                                     aws_secret_access_key=secret_key)
    else:
        logger.info('Using stored AWS profile for client')
        sess = boto3.session.Session(profile_name=profile_name)
    client = sess.client('s3')
    return client


def stmts_to_jsonl_str(stmts):
    return '\n'.join([json.dumps(stmt) for stmt in stmts_to_json(stmts)])
