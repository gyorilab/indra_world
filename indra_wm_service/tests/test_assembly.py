from indra.statements import Event, Concept
from indra.tools import assemble_corpus as ac
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


def test_compositional_refinements():
    def make_event(comp_grounding):
        scored_grounding = [
            (gr, 1.0) if gr else None
            for gr in comp_grounding
        ]
        concept = Concept(name='x',
                          db_refs={'WM': [scored_grounding]})
        event = Event(concept)
        return event

    wm1 = ('wm_compositional/concept/agriculture',
           'wm_compositional/property/price_or_cost',
           None, None)
    wm2 = ('wm_compositional/concept/agriculture',
           None, None, None)
    wm3 = ('wm_compositional/concept/agriculture/crop',
           None, None, None)
    wm4 = ('wm_compositional/concept/agriculture/crop',
           'wm_compositional/property/price_or_cost',
           None, None)

    events = [make_event(comp_grounding)
              for comp_grounding in [wm1, wm2, wm3, wm4]]

    assembled_stmts = \
        ac.run_preassembly(events,
                           filters=[default_refinement_filter_compositional],
                           ontology=comp_ontology,
                           refinement_fun=compositional_refinement,
                           matches_fun=matches_compositional,
                           return_toplevel=False)
    assert len(assembled_stmts) == 2