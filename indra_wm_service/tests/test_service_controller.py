from copy import deepcopy
from .test_incremental_assembler import s1, s2
from indra_wm_service.controller import ServiceController


def _get_controller():
    sc = ServiceController(db_url='sqlite:///:memory:')
    sc.db.create_all()
    return sc


def test_project():
    sc = _get_controller()
    sc.new_project(0, 'p1', ['d1', 'd2', 'd3'])
    sc.add_dart_record('eidos', '1.0', 'd1', 'xxx', '2020')
    sc.add_dart_record('eidos', '1.0', 'd2', 'yyy', '2020')
    s1x = deepcopy(s1)
    s2x = deepcopy(s2)
    sc.add_prepared_statements([s1x], 'eidos', '1.0', 'd1')
    sc.add_prepared_statements([s2x], 'eidos', '1.0', 'd2')
    sc.load_project(0)
    delta = sc.add_project_documents(0, ['d1'])
    assert not delta
    sc.add_dart_record('sofia', '1.0', 'd1', 'xx1', '2020')
    sc.add_dart_record('hume', '1.0', 'd1', 'xx2', '2020')
    delta = sc.check_assembly_triggers_for_project(0)
    assert delta