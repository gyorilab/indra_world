import json
import logging
from indra.config import get_config
from indra_world.sources.dart import DartClient
from indra_world.service.corpus_manager import CorpusManager


logger = logging.getLogger('august_embeds')


if __name__ == '__main__':
    output_version = '3.0'
    db_url = get_config('INDRA_WM_SERVICE_DB')
    readers = ['eidos', 'hume', 'sofia']
    logger.info('Using DB URL %s' % db_url)
    dc = DartClient()
    records = dc.get_reader_output_records(readers=readers)
    logger.info('Got a total of %d records' % len(records))
    records = [rec for rec in records
               if rec['output_version'] == output_version]
    logger.info('Got %d records for version %s' %
                (len(records), output_version))
    tenants = {
        'new-america': {'name': 'New America'},
        'ata': {'name': 'ATA'}
        }
    for tenant, properties in tenants.items():
        tenant_records = [rec for rec in records if rec.get('tenants') and
                          tenant in rec['tenants']]
        logger.info('Got %d records for tenant %s' % (len(tenant_records),
                                                      tenant))
        corpus_id = 'august_embed_%s' % tenant
        corpus_name = 'August embed %s' % properties['name']

        num_docs = len({rec['document_id'] for rec in tenant_records})
        logger.info('Got %d unique documents' % num_docs)

        meta_data = {
            'corpus_id': corpus_id,
            'description': 'August embed seed corpus for %s' %
                properties['name'],
            'display_name': corpus_name,
            'readers': readers,
            'assembly': {
                'level': 'grounding_location',
                'grounding_threshold': 0.6,
            },
            'num_statements': 0,
            'num_documents': num_docs
        }

        logger.info('Using metadata: %s' % json.dumps(meta_data, indent=1))

        cm = CorpusManager(
            db_url=db_url,
            dart_records=tenant_records,
            corpus_id=corpus_id,
            metadata=meta_data,
            dart_client=DartClient(),
        )
        logger.info('Preparing corpus statements')
        cm.prepare(records_exist=True)
        logger.info('Assembling statements')
        cm.assemble()
        logger.info('Dumping statements to S3')
        cm.dump_s3()
