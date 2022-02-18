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
    readers = ['eidos', 'hume']
    recs = [
        r for r in dc.get_reader_output_records(readers)
        if r['output_version'] == ontology_version
    ]


    ont_json = dc.get_ontology(ontology_version)
    ontology = \
        WorldOntology(url=None,
                      yml=yaml.load(ont_json['ontology'],
                                    Loader=yaml.FullLoader))
    ontology.initialize()

    corpus_id = 'feb2022_initial_dsmte'
    meta_data = {
        'corpus_id': corpus_id,
        'description': 'February 2022 embed initial corpus for DSMT-E',
        'display_name': 'Feb. 2022 Initial DSMT-E',
        'readers': readers,
        'assembly': {
            'level': 'grounding_location',
            'grounding_threshold': 0.6,
        },
        'num_statements': 0,
        'num_documents': 272
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
