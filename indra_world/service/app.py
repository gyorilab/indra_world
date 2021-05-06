import logging
import json
from indra.config import get_config
from indra.statements import stmts_to_json
from flask import Flask, request, abort
from flask_restx import Api, Resource, fields
from .controller import ServiceController
from ..sources.dart import DartClient
from ..sources import hume, cwms, sofia, eidos

logger = logging.getLogger('indra_world.service.app')

db_url = get_config('INDRA_WM_SERVICE_DB', failure_ok=False)
if not get_config('DART_WM_URL'):
    dart_client = DartClient(storage_mode='local')
else:
    dart_client = DartClient(storage_mode='web')
sc = ServiceController(db_url, dart_client=dart_client)


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
readers_ns = api.namespace('Readers endpoints',
                           'Readers endpoints',
                           path='/')

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

curation_model_wrapped = api.model(
    'CurationWrapped',
    {
        '12345': fields.Nested(curation_model)
    }
)

submit_curations_model = api.model(
    'SubmitCurations',
    {'project_id': fields.String(example='project1'),
     'curations': fields.List(fields.Nested(curation_model_wrapped))
     }
)

new_project_model = api.model(
    'NewProject',
    {'project_id': fields.String(example='project1', required=True),
     'project_name': fields.String(example='Project 1', required=True),
     'corpus_id': fields.String(example='corpus1', required=False)
     }
)

wm_text_model = api.model(
    'WMText',
    {'text': fields.String(example='Rainfall causes floods.')})

jsonld_model = api.model(
    'jsonld',
    {'jsonld': fields.String(example='{}')})

eidos_text_model = api.inherit('EidosText', wm_text_model, {
    'webservice': fields.String,
    'grounding_ns': fields.List(fields.String(example=['WM']), required=False),
    'extract_filter': fields.List(fields.String(example=['Influence']),
                                  required=False),
    'grounding_mode': fields.String(example='flat', required=False)
})

eidos_jsonld_model = api.inherit('EidosJson', jsonld_model, {
    'grounding_ns': fields.List(fields.String(example=['WM']), required=False),
    'extract_filter': fields.List(fields.String(example=['Influence']),
                                  required=False),
    'grounding_mode': fields.String(example='flat', required=False)    
})

sofia_json_model = api.model(
    'json',
    {'json': fields.String(example='{}'),
     'extract_filter': fields.List(fields.String(example=['Influence']),
                                   required=False),
     'grounding_mode': fields.String(example='flat', required=False)  
    })

def _stmts_from_proc(proc):
    if proc and proc.statements:
        stmts = stmts_to_json(proc.statements)
        res = {'statements': stmts}
    else:
        res = {'statements': []}
    return res


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
        """Add and process DART record.

        Parameters
        ----------
        identity : str
            Name of the reader.

        version : str
            Reader version.

        document_id : str
            ID of a document to process.

        storage_key : str
            Key to store the record with.
        """
        record = {k: request.json[k] for k in ['identity', 'version',
                                               'document_id', 'storage_key']}
        logger.info('Got notification for DART record: %s' % str(record))
        res = sc.add_dart_record(record)
        if res is None:
            abort(400, 'The record could not be added, possibly because '
                       'it\'s a duplicate.')
        sc.process_dart_record(record)
        return 'OK'


@assembly_ns.expect(new_project_model)
@assembly_ns.route('/new_project')
class NewProject(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Create new project.

        Parameters
        ----------
        project_id : str
            ID of a new project.

        project_name : str
            Name of a new project.

        corpus_id : str
            ID of a corpus.
        """
        project_id = request.json.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        project_name = request.json.get('project_name')
        corpus_id = request.json.get('corpus_id')
        logger.info('Got new project request: %s, %s, %s' %
                    (project_id, project_name, corpus_id))
        sc.new_project(project_id, project_name, corpus_id=corpus_id)


@assembly_ns.expect(project_records_model)
@assembly_ns.route('/add_project_records')
class AddProjectRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Add project records and assemble them.

        Parameters
        ----------
        project_id : str
            ID of a project to add records.

        records : list[dict]
            A list of records to add, each should have a 'storage_key'.

        Returns
        -------
        delta_json : json
            A JSON representation of AssemblyDelta.
        """
        project_id = request.json.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        records = request.json.get('records')
        record_keys = [rec['storage_key'] for rec in records]
        sc.add_project_records(project_id, record_keys)
        logger.info('Got assembly request for project %s with %d records' %
                    (project_id, len(record_keys)))
        delta = sc.assemble_new_records(project_id,
                                        new_record_keys=record_keys)
        return delta.to_json()


@assembly_ns.route('/get_projects')
class GetProjects(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        """Get a list of all projects."""
        projects = sc.get_projects()
        return projects


@assembly_ns.expect(project_model)
@assembly_ns.route('/get_project_records')
class GetProjectRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        """Get records for a project.

        Paremeters
        ----------
        project_id : str
            ID of a project.

        Returns
        -------
        records : list[dict]
            A list of records for the project.
        """
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
        """Submit curations.

        Parameters
        ----------
        project_id : str
            ID of a project.
        curations : list[dict]
            A list of curations to submit.
        """
        # TODO: previously, each curation contained a project ID as an attribute
        # we need to check if it's possible that curations are submitted for
        # multiple projects at once.
        project_id = request.json.get('project_id')
        curations = request.json.get('curations')
        logger.info('Got %d curations for project %s' %
                    (len(curations), project_id))
        for stmt_id, curation in curations.items():
            sc.add_curation(project_id, curation)


@assembly_ns.expect(project_model)
@assembly_ns.route('/get_project_curations')
class GetProjectCurations(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def get(self):
        """Get project curations.

        Parameters
        ----------
        project_id : str
            ID of a project.

        Returns
        -------
        curations : list[dict]
            A list of curations for the project.
        """
        project_id = request.json.get('project_id')
        curations = sc.get_project_curations(project_id)
        return curations


# Hume
@readers_ns.expect(jsonld_model)
@readers_ns.route('/hume/process_jsonld')
class HumeProcessJsonld(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Process Hume JSON-LD and return INDRA Statements.

        Parameters
        ----------
        jsonld : str
            The JSON-LD string to be processed.

        Returns
        -------
        statements : list[indra.statements.Statement.to_json()]
            A list of extracted INDRA Statements.
        """
        args = request.json
        jsonld_str = args.get('jsonld')
        jsonld = json.loads(jsonld_str)
        hp = hume.process_jsonld(jsonld)
        return _stmts_from_proc(hp)


# CWMS
@readers_ns.expect(wm_text_model)
@readers_ns.route('/cwms/process_text')
class CwmsProcessText(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Process text with CWMS and return INDRA Statements.

        Parameters
        ----------
        text : str
            Text to process

        Returns
        -------
        statements : list[indra.statements.Statement.to_json()]
            A list of extracted INDRA Statements.
        """
        args = request.json
        text = args.get('text')
        cp = cwms.process_text(text)
        return _stmts_from_proc(cp)


# Hide docs until webservice is available
@readers_ns.expect(eidos_text_model)
@readers_ns.route('/eidos/process_text', doc=False)
class EidosProcessText(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Process text with EIDOS and return INDRA Statements.

        Parameters
        ----------
        text : str
            The text to be processed.

        webservice : Optional[str]
            An Eidos reader web service URL to send the request to.
            If None, the reading is assumed to be done with the Eidos JAR
            rather than via a web service. Default: None

        grounding_ns : Optional[list]
            A list of name spaces for which INDRA should represent groundings, 
            when given. If not specified or None, all grounding name spaces are
            propagated. If an empty list, no groundings are propagated.
            Example: ['UN', 'WM'], Default: None

        extract_filter : Optional[list]
            A list of relation types to extract. Valid values in the list are
            'influence', 'association', 'event'. If not given, all relation
            types are extracted. This argument can be used if, for instance,
            only Influence statements are of interest. Default: None

        grounding_mode : Optional[str]
            Selects whether 'flat' or 'compositional' groundings should be
            extracted. Default: 'flat'.

        Returns
        -------
        statements : list[indra.statements.Statement.to_json()]
            A list of extracted INDRA Statements.
        """
        args = request.json
        text = args.get('text')
        webservice = args.get('webservice')
        if not webservice:
            abort(400, 'No web service address provided.')
        grounding_ns = args.get('grounding_ns')
        extract_filter = args.get('extract_filter')
        grounding_mode = args.get('grounding_mode')
        ep = eidos.process_text(
            text, webservice=webservice, grounding_ns=grounding_ns,
            extract_filter=extract_filter, grounding_mode=grounding_mode)
        return _stmts_from_proc(ep)


@readers_ns.expect(jsonld_model)
@readers_ns.route('/eidos/process_jsonld')
class EidosProcessJsonld(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Process an EIDOS JSON-LD and return INDRA Statements.

        Parameters
        ----------
        jsonld : str
            The JSON-LD string to be processed.

        grounding_ns : Optional[list]
            A list of name spaces for which INDRA should represent groundings, 
            when given. If not specified or None, all grounding name spaces are
            propagated. If an empty list, no groundings are propagated.
            Example: ['UN', 'WM'], Default: None

        extract_filter : Optional[list]
            A list of relation types to extract. Valid values in the list are
            'influence', 'association', 'event'. If not given, all relation
            types are extracted. This argument can be used if, for instance,
            only Influence statements are of interest. Default: None

        grounding_mode : Optional[str]
            Selects whether 'flat' or 'compositional' groundings should be
            extracted. Default: 'flat'.

        Returns
        -------
        statements : list[indra.statements.Statement.to_json()]
            A list of extracted INDRA Statements.
        """
        args = request.json
        eidos_json = args.get('jsonld')
        grounding_ns = args.get('grounding_ns')
        extract_filter = args.get('extract_filter')
        grounding_mode = args.get('grounding_mode')
        jj = json.loads(eidos_json)
        ep = eidos.process_json(
            jj, grounding_ns=grounding_ns, extract_filter=extract_filter,
            grounding_mode=grounding_mode)
        return _stmts_from_proc(ep)


@readers_ns.expect(sofia_json_model)
@readers_ns.route('/sofia/process_json')
class SofiaProcessJson(Resource):
    @api.doc(False)
    def options(self):
        return {}

    def post(self):
        """Process a Sofia JSON and return INDRA Statements.

        Parameters
        ----------
        json : str
            The JSON string to be processed.

        extract_filter : Optional[list]
            A list of relation types to extract. Valid values in the list are
            'influence', 'association', 'event'. If not given, all relation
            types are extracted. This argument can be used if, for instance,
            only Influence statements are of interest. Default: None

        grounding_mode : Optional[str]
            Selects whether 'flat' or 'compositional' groundings should be
            extracted. Default: 'flat'.

        Returns
        -------
        statements : list[indra.statements.Statement.to_json()]
            A list of extracted INDRA Statements.
        """
        args = request.json
        sofia_json = args.get('json')
        extract_filter = args.get('extract_filter')
        grounding_mode = args.get('grounding_mode')
        jj = json.loads(sofia_json)
        ep = sofia.process_json(
            jj, extract_filter=extract_filter, grounding_mode=grounding_mode)
        return _stmts_from_proc(ep)


if __name__ == '__main__':
    app.run()
