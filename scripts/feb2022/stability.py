import json
import logging
from indra_world.sources.dart import DartClient, print_record_stats
from indra.config import get_config
from indra_world.service.corpus_manager import CorpusManager


# These settings define the scope of the KB
ontology_version = 'e0925975-e85e-47e6-aa62-befc16d792fd'
tenant = 'stability'
reader_versions = {'eidos': 'feb2022expV2', 'hume': 'R2022_03_15_3',
                   'sofia': 'march2022exp'}


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    # STEP 1: Collect records based on defined scope
    dc = DartClient()
    recs = dc.get_reader_output_records(
        readers=list(reader_versions.keys()),
        #versions=list(reader_versions.values()),
        ontology_id=ontology_version,
        tenant=tenant,
        unique=True)

    recs = [r for r in recs if (reader_versions[r['identity']] is None or
                                reader_versions[r['identity']] == r['version'])]

    print_record_stats(recs)

    # STEP 2: Define metadata for use by CauseMos
    version = '1'
    corpus_id = 'mar2022_%s_v%s' % (tenant, version)
    meta_data = {
        'corpus_id': corpus_id,
        'description': 'March 2022 embed corpus for %s v%s' % (tenant, version),
        'display_name': 'Mar. 2022 %s v%s' % (tenant, version),
        'readers': list(reader_versions.keys()),
        'assembly': {
            'level': 'grounding_location',
            'grounding_threshold': 0.6,
        },
        'num_statements': 0,
        'num_documents': len({r['document_id'] for r in recs})
    }

    logger.info('Using metadata: %s' % json.dumps(meta_data, indent=1))

    # STEP 3: Create a CorpusManager and run it to create and upload the corpus
    ontology = dc.get_ontology_graph(ontology_version)
    ontology.initialize()

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
