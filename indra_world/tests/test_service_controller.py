import os
from copy import deepcopy
from .test_incremental_assembler import s1, s2
from indra_world.sources.dart import DartClient
from indra_world.service.controller import ServiceController

HERE = os.path.dirname(os.path.abspath(__file__))


def _get_eidos_output():
    fname = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data',
                         'eidos', 'eidos_compositional.jsonld')
    with open(fname, 'r') as fh:
        return fh.read()


def _get_controller():
    local_storage = os.path.join(HERE, 'dart')
    dart_client = DartClient(storage_mode='local',
                             local_storage=local_storage)
    sc = ServiceController(db_url='sqlite:///:memory:',
                           dart_client=dart_client)
    sc.db.create_all()
    return sc


def test_new_blank_project_load():
    sc = _get_controller()
    # Start a new project and load it with no statements
    sc.new_project('p1', 'my project')
    sc.load_project('p1')
    assert len(sc.assemblers) == 1
    assert not sc.assemblers['p1'].stmts_by_hash
    # The project is now removed
    sc.unload_project('p1')
    assert not sc.assemblers
    # Nothing bad should happen here
    sc.unload_project('p1')
    assert not sc.assemblers


def test_new_project_with_statements_load():
    sc = _get_controller()
    # Start a new project and add some statements to it
    sc.new_project('p1', 'my project')
    rec1 = {'identity': 'eidos',
            'version': '1.0',
            'document_id': 'd1',
            'storage_key': 'xxx',
            'output_version': '1.2',
            'tenants': 'a|b'}
    rec2 = {'identity': 'eidos',
            'version': '1.0',
            'document_id': 'd2',
            'storage_key': 'yyy',
            'output_version': '1.2',
            'labels': 'x|y'}
    sc.add_dart_record(rec1)
    sc.add_dart_record(rec2)
    s1x = deepcopy(s1)
    s2x = deepcopy(s2)
    sc.add_prepared_statements([s1x], rec1['storage_key'])
    sc.add_prepared_statements([s2x], rec2['storage_key'])
    sc.add_project_records('p1', [rec1['storage_key'], rec2['storage_key']])
    # Now load the project and ckeck that it was correctly initialized
    sc.load_project('p1')
    assert len(sc.assemblers) == 1
    assert len(sc.assemblers['p1'].stmts_by_hash) == 2, \
        sc.assemblers['p1'].stmts_by_hash


def test_add_reader_output():
    sc = _get_controller()
    sc.new_project('p1', 'my_project')
    rec = {'identity': 'eidos',
           'version': '1.0',
           'document_id': 'd1',
           'storage_key': 'xxx',
           'output_version': '1.2'}
    sc.add_dart_record(rec, '2020')
    sc.add_project_records('p1', ['xxx'])
    eidos_output = _get_eidos_output()
    sc.add_reader_output(eidos_output, rec)
    sc.load_project('p1')
    assert len(sc.assemblers['p1'].prepared_stmts) == 1, \
        sc.assemblers['p1'].prepared_stmts


def test_project():
    sc = _get_controller()
    # Start a new project with no documents
    sc.new_project('p1', 'my project')
    # Add outputs for 2 documents
    sc.add_dart_record(
        {'identity': 'eidos',
         'version': '1.0',
         'document_id': 'd1',
         'storage_key': 'xxx',
         'output_version': '1.2'},
        '2020'
    )
    sc.add_dart_record(
        {'identity': 'eidos',
         'version': '1.0',
         'document_id': 'd2',
         'storage_key': 'yyy',
         'output_version': '1.2'},
        '2020'
    )
    s1x = deepcopy(s1)
    s2x = deepcopy(s2)
    # Add statements from these two documents
    sc.add_prepared_statements([s1x], 'xxx')
    sc.add_prepared_statements([s2x], 'yyy')
    # Now load the project and add one of the documents to the project
    sc.add_project_records('p1', ['xxx', 'yyy'])
    # For this test we assume that yyy is old and xxx is new
    delta = sc.assemble_new_records('p1', ['xxx'])
    assert delta is not None
    # We get one new statement
    assert set(delta.new_stmts) == {s1x.get_hash()}
    # We get one new evidence for the statement
    assert set(delta.new_evidences) == {s1x.get_hash()}
    assert not delta.new_refinements
    assert set(delta.beliefs) == {s1x.get_hash(), s2x.get_hash()}


def test_duplicate_record():
    sc = _get_controller()

    rec = {'identity': 'eidos',
           'version': '1.0',
           'document_id': 'd1',
           'storage_key': 'xxx',
           'output_version': '1.2'}
    res = sc.add_dart_record(rec, '2020')
    assert res.get('rowcount') == 1
    res = sc.add_dart_record(rec, '2020')
    assert res is None


def test_get_projects():
    sc = _get_controller()
    sc.new_project('p1', 'Project 1')
    projects = sc.get_projects()
    assert len(projects) == 1
    assert projects[0] == {'id': 'p1', 'name': 'Project 1'}
