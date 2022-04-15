import os.path
from collections import Counter
import json
import logging

from indra.config import get_config
from indra.statements import stmts_to_json
from flask import Flask, request, abort
from flask_bootstrap import Bootstrap
from flask_restx import Api, Resource, fields, reqparse
from .controller import ServiceController
from .corpus_manager import CorpusManager
from ..sources.dart import DartClient, get_record_key
from ..sources import hume, cwms, sofia, eidos

logger = logging.getLogger('indra_world.service.app')

db_url = get_config('INDRA_WM_SERVICE_DB', failure_ok=False)
if not get_config('DART_WM_URL'):
    dart_client = DartClient(storage_mode='local')
else:
    dart_client = DartClient(storage_mode='web')
sc = ServiceController(db_url, dart_client=dart_client)

VERSION = '3.0'


app = Flask(__name__)
Bootstrap(app)
app.config['RESTX_MASK_SWAGGER'] = False
app.config["SWAGGER_UI_DOC_EXPANSION"] = "list"
app.config['SECRET_KEY'] = 'dev_key'
api = Api(app, title='INDRA World Modelers API',
          description='REST API for INDRA World Modelers',
          version=VERSION)

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
sources_ns = api.namespace('Sources endpoints',
                           'Sources endpoints',
                           path='/sources')

# Models
dict_model = api.model('dict', {})

dart_record_model = api.model(
    'DartRecord',
    {'identity': fields.String(example='eidos',
                               description='Name of the reader'),
     'version': fields.String(example='1.0', description='Reader version'),
     'document_id': fields.String(
         example='70a62e43-f881-47b1-8367-a3cca9450c03',
         description='ID of a document to process'),
     'storage_key': fields.String(
         example='bcd04c45-3cfc-456f-a31e-59e875aefabf.json',
         description='Key to store the record with'),
     'output_version': fields.String(
         example='1.2',
         description='The output version for the reader output.'
     ),
     'tenants': fields.List(fields.String, example=['tenant1'],
                            description='Tenants associated with the output.'),
     'labels': fields.List(fields.String, example=['label1', 'label2'],
                           description='Labels associated with the output.')
    }
)

project_request_model = reqparse.RequestParser()
project_request_model.add_argument('project_id', type=str, required=True,
                                   help='ID of a project')

project_records_model = api.model(
    'ProjectRecords',
    {'project_id': fields.String(example='project1',
                                 description='ID of a project'),
     'records': fields.List(
        fields.Nested(dart_record_model),
        description='A list of records to add')
     }
)

curation_model = api.model(
    'Curation',
    {
        'project_id': fields.String(example='project1',
                                    description='ID of a project'),
        'statement_id': fields.String(
            example='83f5aec2-978b-4e01-a2c9-e231f90bfabd',
            description='INDRA Statement ID'),
        'update_type': fields.String(example='discard_statement',
                                     description='The curation operation '
                                                 'applied to the Statement')
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
    {'project_id': fields.String(example='project1',
                                 description='ID of a project'),
     'curations': fields.Nested(curation_model_wrapped)
     }
)

new_project_model = api.model(
    'NewProject',
    {'project_id': fields.String(example='project1', required=True,
                                 description='ID of a project'),
     'project_name': fields.String(example='Project 1', required=True,
                                   description='Name of a project'),
     'corpus_id': fields.String(example='corpus1', required=False,
                                description='ID of a corpus')
     }
)

wm_text_model = api.model(
    'WMText',
    {'text': fields.String(example='Rainfall causes floods.',
                           description='Text to process')})

jsonld_model = api.model(
    'jsonld',
    {'jsonld': fields.String(example='{}', description='JSON-LD reader output')})

eidos_text_model = api.inherit('EidosText', wm_text_model, {
    'webservice': fields.String(description='URL for Eidos webservice'),
    'grounding_ns': fields.List(
        fields.String, example=['WM'], required=False,
        description='A list of name spaces for which INDRA should represent groundings'),
    'extract_filter': fields.List(
        fields.String, example=['influence'], required=False,
        description='A list of relation types to extract'),
    'grounding_mode': fields.String(example='flat', required=False)
})

eidos_jsonld_model = api.inherit('EidosJson', jsonld_model, {
    'grounding_ns': fields.List(
        fields.String, example=['WM'], required=False,
        description='A list of name spaces for which INDRA should '
                    'represent groundings'),
    'extract_filter': fields.List(
        fields.String, example=['influence'], required=False,
        description='A list of relation types to extract'),
    'grounding_mode': fields.String(example='flat', required=False,
                                    description='flat or compositional')
})

sofia_json_model = api.model(
    'json',
    {'json': fields.String(example='{}', description='JSON reader output'),
     'extract_filter': fields.List(
         fields.String(example=['influence']), required=False,
         description='A list of relation types to extract'),
     'grounding_mode': fields.String(example='flat', required=False,
                                     description='flat or compositional')  
    })

# Models for response
health_model = api.model('Health', {
    'state': fields.String(example='healthy'),
    'version': fields.String(example='1.0.0')
})

project_resp_model = api.model('ProjectResponse', {
    'id': fields.String(example='project1', description='Project ID'),
    'name': fields.String(example='Project 1', description='Project name')
})

project_records_resp = fields.List(
    fields.String, example=['bcd04c45-3cfc-456f-a31e-59e875aefabf.json'])

curated_mapping = fields.Raw(example={'12345': '23456'})

project_curation = fields.Wildcard(fields.Nested(curation_model), example={
    '12345': {
        'project_id': 'project1',
        'statement_id': '83f5aec2-978b-4e01-a2c9-e231f90bfabd',
        'update_type': 'discard_statement'
    },
})

stmt_fields = fields.Raw(example={
    "id": "acc6d47c-f622-41a4-8ae9-d7b0f3d24a2f",
    "type": "Influence",
    "subj": {"db_refs": {"WM": "wm/concept/causal_factor/environmental/meteorologic/precipitation/rainfall"}, "name": "rainfall"},
    "obj": {"db_refs": {"WM": "wm/concept/causal_factor/crisis_and_disaster/environmental_disasters/natural_disaster/flooding"}, "name": "flood"},
    "evidence": [{"text": "Rainfall causes flood", "source_api": "eidos"}]
}, description='INDRA Statement JSON')

stmts_model = api.model('Statements', {
    'statements': fields.List(stmt_fields)
})

delta_fields = fields.Raw(example={
    'new_statements': {
        '12345': {
            "id": "acc6d47c-f622-41a4-8ae9-d7b0f3d24a2f",
            "type": "Influence",
            "subj": {"db_refs": {"WM": "wm/concept/causal_factor/environmental/meteorologic/precipitation/rainfall"}, "name": "rainfall"},
            "obj": {"db_refs": {"WM": "wm/concept/causal_factor/crisis_and_disaster/environmental_disasters/natural_disaster/flooding"}, "name": "flood"},
            "evidence": [{"text": "Rainfall causes flood", "source_api": "eidos"}]
            }
    },
    'new_evidence': {
        '12345': [{"text": "Rainfall causes flood", "source_api": "eidos"}]
    },
    'new_refinements': [['12345', '23456'], ['34567', '45678']],
    'beliefs': {'12345': 0.7, '23456': 0.9}
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

    @base_ns.response(200, 'State and version of the API', health_model)
    def get(self):
        return {'state': 'healthy', 'version': VERSION}


# notify
@dart_ns.expect(dart_record_model)
@dart_ns.route('/notify')
class Notify(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @dart_ns.response(200, 'DART record is added and processed')
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

        output_version : str
            The output version (typically ontology version).

        labels : list of str
            A list of labels for the output.

        tenants : list of str
            A list of tenants for the output.
        """
        # We take these parameters as is
        record = {k: request.json[k] for k in ['identity', 'version',
                                               'document_id', 'storage_key',
                                               'output_version']}
        # Here we stringify any lists
        for key in ['labels', 'tenants']:
            record[key] = None if key not in request.json \
                else '|'.join(request.json[key])

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

    @assembly_ns.response(200, 'New project is created')
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

    @assembly_ns.response(200, 'AssemblyDelta JSON', delta_fields)
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
        logger.info('Finished constructing assembly delta.')
        delta_json = delta.to_json()
        logger.info('Finished JSON-serializing assembly delta, returning')
        return delta_json


@assembly_ns.route('/get_projects')
class GetProjects(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @assembly_ns.marshal_list_with(project_resp_model,
                                   description='List of projects')
    def get(self):
        """Get a list of all projects.

        Returns
        -------
        records : list[dict]
            A list of projects.
        """
        projects = sc.get_projects()
        return projects


@assembly_ns.expect(project_request_model)
@assembly_ns.route('/get_project_records', methods=['GET'])
class GetProjectRecords(Resource):
    @assembly_ns.response(200, 'A list of records', project_records_resp)
    def get(self):
        """Get records for a project.

        Parameters
        ----------
        project_id : str
            ID of a project.

        Returns
        -------
        records : list[dict]
            A list of records for the project.
        """
        project_id = request.args.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        records = sc.get_project_records(project_id)
        return records


@assembly_ns.route('/get_all_records')
class GetAllRecords(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @assembly_ns.response(200, 'A list of records', project_records_resp)
    def get(self):
        """Get all DART records captured by the service.

        Returns
        -------
        records : list[dict]
            A list of records.
        """
        records = sc.get_all_records()
        return records


@assembly_ns.expect(submit_curations_model)
@assembly_ns.route('/submit_curations')
class SubmitCurations(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @assembly_ns.response(200, description=(
        'Mapping from old statement hashes to new statement hashes '
        'if they changed due to the curations'), model=curated_mapping)
    def post(self):
        """Submit curations.

        Parameters
        ----------
        project_id : str
            ID of a project.
        curations : list[dict]
            A list of curations to submit.
        calculate_mappings : bool
            Whether to calculate and return a mappings data structure
            representing the changes made by the curations to the overall
            project corpus. This is False by default since for large
            projects this calculation can take a long time.

        Returns
        -------
        mappings : dict
            For any statement matches hashes that have changed due to the
            curations submitted here, the new hash (after applying the curation)
            is given. Statements whose hash didn't change, or if a curation
            for some reason couldn't be applied, the given statement is
            not added to the return value.
        """
        project_id = request.json.get('project_id')
        curations = request.json.get('curations')
        calculate_mappings = request.json.get('calculate_mappings', False)
        logger.info('Got %d curations for project %s' %
                    (len(curations), project_id))
        # Convert to int hashes here
        curations = {int(sh): cur for sh, cur in curations.items()}
        mappings = sc.add_curations(project_id, curations,
                                    calculate_mappings=calculate_mappings)
        # Convert back to strings for consistency
        return {str(nh): str(oh) for nh, oh in mappings.items()}


@assembly_ns.expect(project_request_model)
@assembly_ns.route('/get_project_curations')
class GetProjectCurations(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @assembly_ns.response(200, 'Mapping from statement hashes to curations',
                          project_curation)
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
        project_id = request.args.get('project_id')
        if not project_id:
            abort(400, 'The project_id parameter is missing or empty.')
        curations = sc.get_project_curations(project_id)
        return curations


# Hume
@sources_ns.expect(jsonld_model)
@sources_ns.route('/hume/process_jsonld')
class HumeProcessJsonld(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @sources_ns.response(200, 'INDRA Statements', stmts_model)
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
@sources_ns.expect(wm_text_model)
@sources_ns.route('/cwms/process_text')
class CwmsProcessText(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @sources_ns.response(200, 'INDRA Statements', stmts_model)
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
@sources_ns.expect(eidos_text_model)
@sources_ns.route('/eidos/process_text', doc=False)
class EidosProcessText(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @sources_ns.response(200, 'INDRA Statements', stmts_model)
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


@sources_ns.expect(eidos_jsonld_model)
@sources_ns.route('/eidos/process_jsonld')
class EidosProcessJsonld(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @sources_ns.response(200, 'INDRA Statements', stmts_model)
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


@sources_ns.expect(sofia_json_model)
@sources_ns.route('/sofia/process_json')
class SofiaProcessJson(Resource):
    @api.doc(False)
    def options(self):
        return {}

    @sources_ns.response(200, 'INDRA Statements', stmts_model)
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


### Dashboard app
if os.environ.get('LOCAL_DEPLOYMENT'):
    reader_names = [('eidos', 'eidos'),
                    ('hume', 'hume'),
                    ('sofia', 'sofia')]

    from wtforms import SubmitField, validators, SelectMultipleField, \
        StringField, TextAreaField
    from wtforms.fields.html5 import DateField
    from flask_wtf import FlaskForm
    from flask import flash, render_template

    def process_reader_versions(data, num_readers):
        if not data:
            return []
        versions = data.split(',')
        if len(versions) != num_readers:
            versions = versions[:num_readers] + \
                [None] * (num_readers - len(versions))
        return versions

    class RecordFinderForm(FlaskForm):
        """Defines the form to find DART records."""

        readers = SelectMultipleField(label='Readers',
                                      id='reader-select',
                                      choices=reader_names,
                                      default=[r for r, _ in reader_names],
                                      validators=[validators.input_required()])
        reader_versions = StringField(label='Reader versions')
        tenant = StringField(label='Tenant ID')
        ontology_id = StringField(label='Ontology ID')
        after_date = DateField(label='After date', format='%Y-%M-%d')
        before_date = DateField(label='Before date', format='%Y-%M-%d')
        query_submit_button = SubmitField('Find reader output records')


    class RunAssemblyForm(FlaskForm):
        """Defines the form for running assembly."""
        corpus_id = StringField(label='Corpus ID',
                                validators=[validators.input_required()])
        corpus_name = StringField(label='Corpus display name',
                                  validators=[validators.input_required()])
        corpus_descr = TextAreaField(label='Corpus description',
                                     validators=[validators.input_required()])
        output_path = StringField(label='Output folder path')
        assembly_submit_button = SubmitField('Run assembly')


    global records


    def _get_record_stats(records):
        stats_rows = [['reader', 'tenants', 'reader_version', 'ontology_version',
                       'count']]
        for (reader, tenants, reader_version, ontology_version), count in sorted(
                Counter([get_record_key(rec) for rec in records]).items(),
                key=lambda x: x[1], reverse=True):
            stats_rows.append([
                reader, '|'.join(tenants), reader_version,
                ontology_version, str(count)])
        record_summary = 'The query returned the following reader output records<br/>'
        record_summary += '<table class="table">' + \
                          '\n'.join(['<tr><td>'
                                     + ('</td><td>'.join(row))
                                     + '</td></tr>' for row in stats_rows]) + \
                          '</table>'
        return record_summary


    @app.route('/dashboard', methods=['GET', 'POST'])
    def dashboard():
        record_finder_form = RecordFinderForm(
            readers=[r for r, _ in reader_names],
        )
        run_assembly_form = RunAssemblyForm()

        state = (record_finder_form.query_submit_button.data,
                 run_assembly_form.assembly_submit_button.data)

        global records
        if state == (False, False):
            return render_template(
                'dashboard.html',
                record_finder_form=record_finder_form,
                run_assembly_form=None,
                record_summary='No record selected yet'
            )
        if state[0] is True:
            timestamp = None if (not record_finder_form.before_date.data
                                 and not record_finder_form.after_date.data) else {}
            if record_finder_form.after_date.data:
                timestamp['after'] = record_finder_form.after_date.data
            if record_finder_form.before_date.data:
                timestamp['before'] = record_finder_form.before_date.data
            records = dart_client.get_reader_output_records(
                readers=record_finder_form.readers.data,
                versions=process_reader_versions(
                    record_finder_form.reader_versions.data,
                    len(record_finder_form.readers.data)
                ),
                tenant=record_finder_form.tenant.data,
                timestamp=timestamp,
                ontology_id=record_finder_form.ontology_id.data,
            )
            return render_template(
                'dashboard.html',
                record_finder_form=record_finder_form,
                run_assembly_form=run_assembly_form,
                record_summary=_get_record_stats(records)
            )
        if state[1] is True:
            corpus_id = run_assembly_form.corpus_id.data
            corpus_name = run_assembly_form.corpus_name.data
            corpus_descr = run_assembly_form.corpus_descr.data
            output_path = run_assembly_form.output_path.data
            if not os.path.exists(output_path):
                flash('Output folder %s doesn\'t exist' % output_path)
                return render_template(
                    'dashboard.html',
                    record_finder_form=record_finder_form,
                    run_assembly_form=run_assembly_form,
                    record_summary=_get_record_stats(records)
                )

            num_docs = len({rec['document_id'] for rec in records})

            meta_data = {
                'corpus_id': corpus_id,
                'description': corpus_descr,
                'display_name': corpus_name,
                'readers': sorted({rec['identity'] for rec in records}),
                'assembly': {
                    'level': 'location',
                    'grounding_threshold': 0.7,
                },
                'num_documents': num_docs
            }

            cm = CorpusManager(db_url=db_url,
                               dart_client=dart_client,
                               dart_records=records,
                               corpus_id=corpus_id,
                               metadata=meta_data)
            cm.prepare(records_exist=True)
            cm.assemble()
            cm.dump_local(output_path, causemos_compatible=True)
            flash('Assembly successful! Outputs (statements.json and '
                  'metadata.json) written to %s.' %
                  os.path.join(output_path, corpus_id))
            return render_template(
                'dashboard.html',
                record_finder_form=record_finder_form,
                run_assembly_form=run_assembly_form,
                record_summary=_get_record_stats(records)
            )
else:
    @app.route('/dashboard', methods=['GET'])
    def dashboard():
        return 'Dashboard only available in local deployment'


if __name__ == '__main__':
    app.run()
