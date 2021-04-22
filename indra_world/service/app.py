from indra.config import get_config
from flask import Flask, request, abort
from flask_restx import Api, Resource, fields
from .controller import ServiceController

db_url = get_config('INDRA_WM_SERVICE_DB', failure_ok=False)
local_storage = get_config('INDRA_WM_CACHE')
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
dict_model = api.model('dict', {})

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

project_model = api.model(
    'Project',
    {'project_id': fields.String(example='project1', required=True)}
)

project_records_model = api.model(
    'ProjectRecords',
    {'project_id': fields.String(example='project1'),
     'records': fields.List(fields.Nested(dart_record_model))
     }
)

curation_model = api.model(
    'Curation',
    {
        'project_id': fields.String(example='project1'),
        'statement_id': fields.String(example='12345'),
        'update_type': fields.String(example='discard_statement')
    }
)

submit_curations_model = api.model(
    'SubmitCurations',
    {'project_id': fields.String(example='project1'),
     'curations': fields.List(fields.Nested(curation_model))
     }
)

new_project_model = api.model(
    'NewProject',
    {'project_id': fields.String(example='project1', required=True),
     'project_name': fields.String(example='Project 1', required=True),
     'corpus_id': fields.String(example='corpus1', required=False)
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
        res = sc.add_dart_record(record)
        if res is None:
            abort(400, 'The record could not be added, possibly because '
                       'it\'s a duplicate.')
        sc.process_dart_record(record, local_storage=local_storage)
        return 'OK'


@assembly_ns.expect(new_project_model)
@assembly_ns.route('/new_project')
class NewProject(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        project_id = request.json.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        project_name = request.json.get('project_name')
        corpus_id = request.json.get('corpus_id')
        sc.new_project(project_id, project_name, corpus_id=corpus_id)


@assembly_ns.expect(project_records_model)
@assembly_ns.route('/add_project_records')
class AddProjectRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        project_id = request.json.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        records = request.json.get('records')
        record_keys = [rec['storage_key'] for rec in records]
        sc.add_project_records(project_id, record_keys)
        delta = sc.assemble_new_records(project_id,
                                        new_record_keys=record_keys)
        return delta.to_json()


@assembly_ns.route('/get_projects')
class GetProjects(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        projects = sc.get_projects()
        return projects


@assembly_ns.expect(project_model)
@assembly_ns.route('/get_project_records')
class GetProjectRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        project_id = request.json.get('project_id')
        records = sc.get_project_records(project_id)
        return records


@assembly_ns.expect(submit_curations_model)
@assembly_ns.route('/submit_curations')
class SubmitCurations(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        # TODO: previously, each curation contained a project ID as an attribute
        # we need to check if it's possible that curations are submitted for
        # multiple projects at once.
        project_id = request.json.get('project_id')
        curations = request.json.get('curations')
        for stmt_id, curation in curations.items():
            sc.add_curation(project_id, curation)


@assembly_ns.route('/get_project_curations')
class GetProjectCurations(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        project_id = request.json.get('project_id')
        curations = sc.get_project_curations(project_id)
        return curations


if __name__ == '__main__':
    app.run()
