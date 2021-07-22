import os
import json
from nose.tools import raises
from datetime import datetime
from indra.statements import stmts_from_json, Influence, Event, Concept, \
    QualitativeDelta
from indra_world.service.app import api, sc, VERSION
from indra_world.sources.dart import DartClient
from indra_world.service.db.manager import DbManager
from .test_service_controller import _get_eidos_output

# Set up the DART client for the service controller
HERE = os.path.dirname(os.path.abspath(__file__))
local_storage = os.path.join(HERE, 'dart')
dart_client = DartClient(storage_mode='local',
                         local_storage=local_storage)
sc.dart_client = dart_client


def _call_api(method, route, *args, **kwargs):
    tc = api.app.test_client()
    req_meth = getattr(tc, method)
    start = datetime.now()
    print("Submitting request to '%s' at %s." % ('/' + route, start))
    print("\targs:", args)
    print("\tkwargs:", kwargs)
    res = req_meth(route, *args, **kwargs)
    end = datetime.now()
    print("Got result with %s at %s after %s seconds."
          % (res.status_code, end, (end-start).total_seconds()))
    if res.status_code != 200:
        raise ValueError(res.status_code)
    return json.loads(res.get_data())


def test_health():
    res = _call_api('get', 'health')
    assert res == {'state': 'healthy', 'version': VERSION}


def test_notify():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _orig = sc.dart_client.get_output_from_record
    sc.dart_client.get_output_from_record = lambda x: _get_eidos_output()

    # Call the service, which will send a request to the server.
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key,
                        output_version='1.2',
                        labels=['l1', 'l2'],
                        tenants=['t1'],
                    ))
    assert res
    records = sc.db.get_dart_records(
        reader='eidos',
        document_id=doc_id,
        reader_version='1.0')
    assert records == [storage_key], records

    full_records = sc.db.get_full_dart_records(
        reader='eidos',
        document_id=doc_id,
        reader_version='1.0')
    assert full_records[0]['tenants'] == 't1'

    stmts = sc.db.get_statements_for_document(document_id=doc_id)
    assert len(stmts) == 1, stmts
    sc.dart_client.get_output_from_record = _orig


@raises(ValueError)
def test_notify_duplicate():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _orig = sc.dart_client.get_output_from_record
    sc.dart_client.get_output_from_record = lambda x: _get_eidos_output()

    # Call the service, which will send a request to the server.
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key
                    ))
    assert res
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key
                    ))
    sc.dart_client.get_output_from_record = _orig


def test_get_projects():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))
    res = _call_api('get', 'assembly/get_projects', json={})
    assert res


def test_get_project_records():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _orig = sc.dart_client.get_output_from_record
    sc.dart_client.get_output_from_record = lambda x: _get_eidos_output()
    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    record = {'identity': 'eidos',
              'version': '1.0',
              'document_id': doc_id,
              'storage_key': storage_key,
              'output_version': '1.2'}
    res = _call_api('post', 'dart/notify', json=record)
    res = _call_api('post', 'assembly/add_project_records',
                    json=dict(
                        project_id='p1',
                        records=[record]
                    ))
    res = _call_api('get', 'assembly/get_project_records',
                    json=dict(project_id='p1'))
    assert res == [storage_key]
    sc.dart_client.get_output_from_record = _orig


def test_curations():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))

    # Now add a record just on the back-end
    sc.db.add_records_for_project('p1', ['r1'])
    # And now add a statement for that record so we can "curate" it
    stmt = Influence(Event(Concept('x')), Event(Concept('y')))
    stmt_hash = -11334164755554266
    sc.db.add_statements_for_record('r1', [stmt], '1.0')

    curation = {'project_id': 'p1',
                'statement_id': 'abcdef',
                'update_type': 'reverse_relation'}
    mappings = _call_api('post', 'assembly/submit_curations',
                         json=dict(
                            project_id='p1',
                            curations={stmt_hash: curation}
                         ))
    assert mappings
    res = _call_api('get', 'assembly/get_project_curations',
                    json=dict(project_id='p1'))
    assert len(res) == 1
    assert res[str(stmt_hash)] == curation, res


def test_cwms_process_text():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    res_json = _call_api('post', 'sources/cwms/process_text',
                         json={'text': 'Hunger causes displacement.'})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1


def test_hume_process_jsonld():
    from indra_world.tests.test_hume import test_file_new_simple
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    with open(test_file_new_simple, 'r') as fh:
        test_jsonld = fh.read()
    res_json = _call_api('post', 'sources/hume/process_jsonld',
                         json={'jsonld': test_jsonld})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1


def test_eidos_json():
    from indra_world.tests.test_eidos import test_jsonld, _get_data_file
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    with open(test_jsonld, 'r') as fh:
        jsonld = fh.read()
    res_json = _call_api('post', 'sources/eidos/process_jsonld',
                         json={'jsonld': jsonld})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1
    stmt = stmts[0]
    assert len(stmt.subj.concept.db_refs) > 2
    assert len(stmt.obj.concept.db_refs) > 2

    # Grounding NS
    res_json = _call_api('post', 'sources/eidos/process_jsonld',
                         json={'jsonld': jsonld, 'grounding_ns': ['UN']})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1
    stmt = stmts[0]
    assert set(stmt.subj.concept.db_refs.keys()) == {'TEXT', 'UN'}
    assert set(stmt.obj.concept.db_refs.keys()) == {'TEXT', 'UN'}

    # Extract filter
    res_json = _call_api('post', 'sources/eidos/process_jsonld',
                         json={'jsonld': jsonld,
                               'extract_filter': ['influence']})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1
    res_json = _call_api('post', 'sources/eidos/process_jsonld',
                         json={'jsonld': jsonld,
                               'extract_filter': ['event']})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 0

    # Grounding mode
    with open(_get_data_file('eidos_compositional.jsonld'), 'r') as fh:
        jsonld = fh.read()
    res_json = _call_api('post', 'sources/eidos/process_jsonld',
                         json={'jsonld': jsonld,
                               'grounding_mode': 'compositional'})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1                             


def test_sofia_json():
    from indra_world.tests.test_sofia import _get_data_file
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    with open(_get_data_file('sofia_test.json'), 'r') as fh:
        test_json = fh.read()
    res_json = _call_api('post', 'sources/sofia/process_json',
                         json={'json': test_json})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 2
    assert isinstance(stmts[0], Influence)
    assert isinstance(stmts[1], Event)

    # Extract filter
    res_json = _call_api('post', 'sources/sofia/process_json',
                         json={'json': test_json,
                               'extract_filter': ['influence']})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 1
    assert isinstance(stmts[0], Influence)

    # Grounding mode
    with open(_get_data_file('sofia_test_comp_no_causal.json'), 'r') as fh:
        test_json = fh.read()
    res_json = _call_api('post', 'sources/sofia/process_json',
                         json={'json': test_json,
                               'grounding_mode': 'compositional'})
    stmts_json = res_json.get('statements')
    stmts = stmts_from_json(stmts_json)
    assert len(stmts) == 2
    assert isinstance(stmts[0], Event)


def test_polarity_curations():
    cur_json = os.path.join(HERE, 'data', 'polarity_curation.json')
    with open(cur_json, 'r') as fh:
        cur = json.load(fh)

    project_id = 'project-3161a0ce-887e-438c-bcf7-50b80746dcd8'

    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id=project_id,
                  project_name='Project 1'
              ))

    # Now add a record just on the back-end
    sc.db.add_records_for_project(project_id, ['r1'])
    # And now add a statement for that record so we can "curate" it
    subj_grounding = {'WM': [[('wm/concept/environment/climate', 1.0), None,
                              None, None]]}
    subj = Event(Concept('climate', db_refs=subj_grounding),
                 delta=QualitativeDelta(polarity=-1))
    obj_grounding = \
        {'WM': [[('wm/concept/crisis_or_disaster/environmental/drought', 1.0),
                 None, None, None]]}
    obj = Event(Concept('drought', db_refs=obj_grounding))
    stmt = Influence(subj, obj)
    sc.db.add_statements_for_record('r1', [stmt], '1.0')

    mappings = _call_api('post', 'assembly/submit_curations',
                         json=cur)
    assert mappings == {'18354331688382610': '-18369311868314428'}, mappings
    res = _call_api('get', 'assembly/get_project_curations',
                    json=dict(project_id=project_id))
    assert len(res) == 1, res
    stmt_hash = 18354331688382610
    assert isinstance(res[str(stmt_hash)], dict)
    assert res[str(stmt_hash)]['after']['subj']['polarity'] == 1


"""
FIXME: IMPLEMENT THIS ENDPOINT

def test_merge_deltas():
    def add_annots(stmt):
        for ev in stmt.evidence:
            ev.annotations['subj_adjectives'] = stmt.subj.delta.adjectives
            ev.annotations['obj_adjectives'] = stmt.obj.delta.adjectives
            ev.annotations['subj_polarity'] = stmt.subj.delta.polarity
            ev.annotations['obj_polarity'] = stmt.obj.delta.polarity
        return stmt

    d1 = QualitativeDelta(polarity=1, adjectives=['a', 'b', 'c'])
    d2 = QualitativeDelta(polarity=-1, adjectives=None)
    d3 = QualitativeDelta(polarity=1, adjectives=['g'])
    d4 = QualitativeDelta(polarity=-1, adjectives=['d', 'e', 'f'])
    d5 = QualitativeDelta(polarity=None, adjectives=['d'])
    d6 = QualitativeDelta(polarity=None, adjectives=None)
    d7 = QualitativeDelta(polarity=1, adjectives=None)

    def make_ev(name, delta):
        return Event(Concept(name), delta=delta)

    route_preassembly = 'preassembly/run_preassembly'
    route_deltas = 'preassembly/merge_deltas'

    stmts = [add_annots(Influence(make_ev('a', sd), make_ev('b', od),
                                  evidence=[Evidence(source_api='eidos',
                                                     text='%d' % idx)]))
             for idx, (sd, od) in enumerate([(d1, d2), (d3, d4)])]
    stmts = _post_stmts_preassembly(
        stmts, route_preassembly, return_toplevel=True)
    stmts = _post_stmts_preassembly(stmts, route_deltas)
    assert stmts[0].subj.delta.polarity == 1, stmts[0].subj.delta
    assert stmts[0].obj.delta.polarity == -1, stmts[0].obj.delta
    assert set(stmts[0].subj.delta.adjectives) == {'a', 'b', 'c', 'g'}, \
        stmts[0].subj.delta
    assert set(stmts[0].obj.delta.adjectives) == {'d', 'e', 'f'}, \
        stmts[0].obj.delta

    stmts = [add_annots(Influence(make_ev('a', sd), make_ev('b', od),
                                  evidence=[Evidence(source_api='eidos',
                                                     text='%d' % idx)]))
             for idx, (sd, od) in enumerate([(d1, d5), (d6, d7), (d6, d7)])]
    stmts = _post_stmts_preassembly(
        stmts, route_preassembly, return_toplevel=True)
    stmts = _post_stmts_preassembly(stmts, route_deltas)
    assert stmts[0].subj.delta.polarity is None, stmts[0].subj.delta
    assert stmts[0].obj.delta.polarity == 1, stmts[0].obj.delta
    assert set(stmts[0].subj.delta.adjectives) == {'a', 'b', 'c'}, \
        stmts[0].subj.delta
    assert set(stmts[0].obj.delta.adjectives) == {'d'}, \
        stmts[0].obj.delta

"""