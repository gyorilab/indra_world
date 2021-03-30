import os
from indra.config import get_config
from flask import Flask, request
from flask_restx import Api, Resource, fields, abort
from indra_wm_service.controller import ServiceController

db_url = get_config('INDRA_WM_SERVICE_DB', failure_ok=False)
sc = ServiceController(db_url)


app = Flask(__name__)
api = Api(app, title='INDRA World Modelers API',
          description='REST API for INDRA World Modelers')

# Namespaces
base_ns = api.namespace('Basic functions',
                        'Basic functions',
                        path='/')
dart_ns = api.namespace('DART endpoints',
                        'DART endpoints',
                        path='/dart')

# Models
dart_record_model = api.model(
    'DartRecord',
    {'identity': fields.String(example='eidos'),
     'version': fields.String(example='1.0'),
     'document_id': fields.String(
         example='70a62e43-f881-47b1-8367-a3cca9450c03'),
     'storage_key': fields.String(
         example='bcd04c45-3cfc-456f-a31e-59e875aefabf.json')
    }
)


# Endpoints to implement
# health
@base_ns.route('/health')
class Health(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        return {'state': 'healthy', 'version': '1.0.0'}


# notify
@dart_ns.expect(dart_record_model)
@dart_ns.route('/notify')
class Notify(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        reader = request.json.get('identity')
        reader_version = request.json.get('version')
        document_id = request.json.get('document_id')
        storage_key = request.json.get('storage_key')
        sc.add_dart_record(reader=reader,
                           reader_version=reader_version,
                           document_id=document_id,
                           storage_key=storage_key)
        sc.process_dart_record(reader=reader,
                               reader_version=reader_version,
                               document_id=document_id,
                               storage_key=storage_key)
        return 'OK'

# download_curations
# submit_curations


if __name__ == '__main__':
    app.run()