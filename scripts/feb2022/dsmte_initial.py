import tqdm
import json
import logging
import requests
from collections import Counter
from indra_world.sources.dart import DartClient
from indra.config import get_config
from indra_world.service.corpus_manager import CorpusManager


ontology_version = '49277ea4-7182-46d2-ba4e-87800ee5a315'
tenant = 'dsmt-e'
reader_versions = {'eidos': 'feb2022expV2', 'hume': 'R2022_03_15_3',
                   'sofia': 'march2022exp'}


logger = logging.getLogger(__name__)


def get_record_key(rec):
    return (rec['identity'], tuple(sorted(rec['tenants'])), rec['version'],
            rec['output_version'])


def print_record_stats(recs):
    print("reader,tenants,reader_version,ontology_version,count")
    for (reader, tenants, reader_version, ontology_version), count in sorted(
            Counter([get_record_key(rec) for rec in recs]).items(),
            key=lambda x: x[1], reverse=True):
        print(
            f"{reader},{'|'.join(tenants)},{reader_version},{ontology_version},{count}")


def get_unique_records(recs):
    return list({(get_record_key(rec), rec['document_id']): rec
                 for rec in recs}.values())


if __name__ == '__main__':
    dc = DartClient()
    readers = list(reader_versions.keys())
    recs = dc.get_reader_output_records(readers)
    recs = [r for r in recs if tenant in r['tenants']]
    recs = [r for r in recs if r['output_version'] == ontology_version]
    recs = [r for r in recs if (reader_versions[r['identity']] is None or
                                reader_versions[r['identity']] == r['version'])]
    recs = get_unique_records(recs)

    print_record_stats(recs)

    #with open('existing_record_keys.txt', 'r')  as f:
    #    existing_record_keys = set([line.strip() for line in f.readlines()])

    #records_not_captured = [r for r in recs
    #                        if r['storage_key'] not in existing_record_keys]
    #for rec in tqdm.tqdm(records_not_captured):
    #    res = requests.post('http://wm.indra.bio/dart/notify', json=rec)
    #    if res.status_code != 200:
    #        print(rec, res.status_code)

    ontology = dc.get_ontology_graph(ontology_version)
    ontology.initialize()

    version = '4'
    corpus_id = 'mar2022_dsmte_v%s' % version
    meta_data = {
        'corpus_id': corpus_id,
        'description': 'March 2022 embed corpus for DSMT-E v%s' % version,
        'display_name': 'Mar. 2022 DSMT-E v%s' % version,
        'readers': readers,
        'assembly': {
            'level': 'grounding_location',
            'grounding_threshold': 0.6,
        },
        'num_statements': 0,
        'num_documents': 20980
    }

    logger.info('Using metadata: %s' % json.dumps(meta_data, indent=1))

    cm = CorpusManager(
        db_url=get_config('INDRA_WM_SERVICE_DB'),
        dart_records=recs,
        corpus_id=corpus_id,
        ontology=ontology,
        metadata=meta_data,
        dart_client=dc,
        tenant=tenant
    )
    logger.info('Preparing corpus statements')
    cm.prepare(records_exist=True)
    logger.info('Assembling statements')
    cm.assemble()
    if cm.assembled_stmts:
        logger.info('Dumping statements to S3')
        cm.dump_s3()
