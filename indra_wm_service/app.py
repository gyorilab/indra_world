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

assembly_ns = api.namespace('Assembly endpoints',
                            'Assembly endpoints',
                            path='/assembly')

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

project_records_model = api.model(
    'ProjectRecords',
    {'project_id': fields.String(example='project1'),
     'records': fields.List(fields.Nested(dart_record_model))
     }
)


new_project_model = api.model(
    'NewProject',
    {'project_id': fields.String(example='project1'),
     'project_name': fields.String(example='Project 1'),
     'corpus_id': fields.String(example='corpus1')
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
        record = {k: request.json[k] for k in ['identity', 'version',
                                               'document_id', 'storage_key']}
        sc.add_dart_record(record)
        sc.process_dart_record(record)
        return 'OK'


@assembly_ns.expect(new_project_model)
@dart_ns.route('/new_project')
class NewProject(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        project_id = request.json.get('project_id')
        project_name = request.json.get('project_name')
        corpus_id = request.json.get('corpus_id')
        sc.new_project(project_id, project_name, corpus_id=corpus_id)


@assembly_ns.expect(project_records_model)
@dart_ns.route('/add_project_records')
class AddProjectRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        project_id = request.json.get('project_id')
        records = request.json.get('records')
        record_keys = [rec['storage_key'] for rec in records]
        sc.add_project_records(project_id, record_keys)
        delta = sc.assemble_new_records(project_id,
                                        new_record_keys=record_keys)
        return delta.to_json()


# download_curations
# submit_curations


if __name__ == '__main__':
    app.run()
