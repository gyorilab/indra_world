from copy import deepcopy
from .test_incremental_assembler import s1, s2
from indra_wm_service.controller import ServiceController


def _get_controller():
    sc = ServiceController(db_url='sqlite:///:memory:')
    sc.db.create_all()
    return sc


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
