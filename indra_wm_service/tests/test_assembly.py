from indra.statements import Event, Concept
from indra_wm_service.assembly.operations import *


def test_compositional_grounding_filder():
    wm = [[('x', 0.5), ('y', 0.8), None, None]]
    concept = Concept('x', db_refs={'WM': wm})
    stmt = Event(concept)
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    assert concept.db_refs['WM'][0][0] == ('y', 0.8), concept.db_refs
    assert concept.db_refs['WM'][0][1] is None

    wm = [[None, ('y', 0.8), None, None]]
    concept.db_refs['WM'] = wm
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    assert concept.db_refs['WM'][0][0] == ('y', 0.8), concept.db_refs
    assert concept.db_refs['WM'][0][1] is None

    wm = [[('x', 0.7), ('y', 0.7), None, None]]
    concept.db_refs['WM'] = wm
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    assert concept.db_refs['WM'][0][0] == ('x', 0.7), concept.db_refs
    assert concept.db_refs['WM'][0][1] == ('y', 0.7), concept.db_refs

    wm = [[('wm_compositional/entity/geo-location', 0.7), ('y', 0.7),
           None, None]]
    concept.db_refs['WM'] = wm
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7,
        ['wm_compositional/entity/geo-location'])
    assert concept.db_refs['WM'][0][0] == ('y', 0.7), concept.db_refs
