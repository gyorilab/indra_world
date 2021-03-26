import os
from copy import deepcopy
from .test_incremental_assembler import s1, s2
from indra_wm_service.controller import ServiceController
import indra

indra_tests_path = os.path.join(indra.__path__[0], 'tests')


def _get_eidos_output():
    fname = os.path.join(indra_tests_path, 'eidos_compositional.jsonld')
    with open(fname, 'r') as fh:
        return fh.read()


def _get_controller():
    sc = ServiceController(db_url='sqlite:///:memory:')
    sc.db.create_all()
    return sc


def test_new_blank_project_load():
    sc = _get_controller()
    # Start a new project and load it with no statements
    sc.new_project(0, 'my_project', doc_ids=['d1', 'd2'])
    sc.load_project(0)
    assert len(sc.assemblers) == 1
    assert not sc.assemblers[0].stmts_by_hash
    # The project is now removed
    sc.unload_project(0)
    assert not sc.assemblers
    # Nothing bad should happen here
    sc.unload_project(0)
    assert not sc.assemblers


def test_new_project_with_statements_load():
    sc = _get_controller()
    # Start a new project and add some statements to it
    sc.new_project(0, 'my_project', doc_ids=['d1', 'd2'])
    sc.add_dart_record('eidos', '1.0', 'd1', 'xxx', '2020')
    s1x = deepcopy(s1)
    s2x = deepcopy(s2)
    sc.add_prepared_statements([s1x], 'eidos', '1.0', 'd1')
    sc.add_prepared_statements([s2x], 'eidos', '1.0', 'd2')
    # Now load the project and ckeck that it was correctly initialized
    sc.load_project(0)
    assert len(sc.assemblers) == 1
    assert len(sc.assemblers[0].stmts_by_hash) == 2


def test_add_reader_output():
    sc = _get_controller()
    sc.new_project(0, 'my_project', doc_ids=[])
    sc.add_project_documents(0, ['d1'])
    sc.add_dart_record('eidos', '1.0', 'd1', 'xxx', '2020')
    eidos_output = _get_eidos_output()
    sc.add_reader_output(eidos_output, 'eidos', '1.0', 'd1')
    sc.load_project(0)


def test_project():
    sc = _get_controller()
    # Start a new project with no documents
    sc.new_project(0, 'p1', doc_ids=[])
    # Add outputs for 2 documents
    sc.add_dart_record('eidos', '1.0', 'd1', 'xxx', '2020')
    sc.add_dart_record('eidos', '1.0', 'd2', 'yyy', '2020')
    s1x = deepcopy(s1)
    s2x = deepcopy(s2)
    # Add statements from these two documents
    sc.add_prepared_statements([s1x], 'eidos', '1.0', 'd1')
    sc.add_prepared_statements([s2x], 'eidos', '1.0', 'd2')
    # Now load the project and add one of the documents to the project
    sc.load_project(0)
    delta = sc.add_project_documents(0, ['d1'])
    # At this point we only have Eidos output for d1 so no delta
    assert not delta
    # We now add records for Sofia and Hume output for d1
    sc.add_dart_record('sofia', '1.0', 'd1', 'xx1', '2020')
    sc.add_dart_record('hume', '1.0', 'd1', 'xx2', '2020')
    # We now satisfy the requirements and generate a delta
    delta = sc.check_assembly_triggers_for_project(0)
    # We get one new statement
    assert set(delta.new_stmts) == {s1x.get_hash()}
    # We get one new evidence for the statement
    assert set(delta.new_evidences) == {s1x.get_hash()}
    assert not delta.new_refinements
    assert set(delta.beliefs) == {s1x.get_hash()}
