import tqdm
import yaml
import json
import logging
from indra_world.sources.dart import DartClient
from indra.config import get_config
from indra_world.ontology import WorldOntology
from indra_world.service.corpus_manager import CorpusManager


ontology_version = 'acab3922-b952-4bc0-9fb1-129659a42baa'
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    dc = DartClient()
    with open('initial_doc_ids.txt', 'r') as fh:
        doc_ids = {l.strip() for l in fh.readlines()}
    recs = dc.get_reader_output_records(['hume'],
        timestamp={'before': '2022-03-01T22:00:00'})
    recs += dc.get_reader_output_records(['eidos'],
        timestamp={'after': '2022-03-02T22:00:00'})
    recs = [r for r in recs if r['doc_id'] in doc_ids]
    recs = [r for r in recs if r['ontology_version'] == ontology_version]

    print(len(recs))

    ontology = dc.get_ontology_graph(ontology_version)
    ontology.initialize()

    corpus_id = 'feb2022_initial_dsmte_v2'
    meta_data = {
        'corpus_id': corpus_id,
        'description': 'February 2022 embed initial corpus for DSMT-E v2 with ' \
            'grounding improvements',
        'display_name': 'Feb. 2022 Initial DSMT-E v2',
        'readers': readers,
        'assembly': {
            'level': 'grounding_location',
            'grounding_threshold': 0.6,
        },
        'num_statements': 0,
        'num_documents': 275
    }

    logger.info('Using metadata: %s' % json.dumps(meta_data, indent=1))

    cm = CorpusManager(
        db_url=get_config('INDRA_WM_SERVICE_DB'),
        dart_records=recs,
        corpus_id='feb2022_initial_dsmte',
        ontology=ontology,
        metadata=meta_data,
        dart_client=dc,
        tenant='dsmt-e'
    )
    logger.info('Preparing corpus statements')
    cm.prepare(records_exist=True)
    logger.info('Assembling statements')
    cm.assemble()
    logger.info('Dumping statements to S3')
    cm.dump_s3()
