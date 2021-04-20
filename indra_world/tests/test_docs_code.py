from indra.statements import Concept, Event, Evidence, Influence
from indra.tools import assemble_corpus as ac
from indra_world.belief import get_eidos_scorer
from indra_world.ontology import world_ontology


# From description of wm stmt assembly pipeline in README.md
def test_readme_wm_pipeline():
    stmts = wm_raw_stmts
    # stmts = ac.filter_grounded_only(stmts)  # Does not work on test stmts
    belief_scorer = get_eidos_scorer()
    stmts = ac.run_preassembly(stmts,
                               return_toplevel=False,
                               belief_scorer=belief_scorer,
                               ontology=world_ontology,
                               normalize_opposites=True,
                               normalize_ns='WM')
    stmts = ac.filter_belief(stmts, 0.8)    # Apply belief cutoff of e.g., 0.8
    assert stmts, 'Update example to yield statements list of non-zero length'


def _make_wm_stmts():
    ev1 = Evidence(source_api='eidos', text='A',
                   annotations={'found_by': 'ported_syntax_1_verb-Causal'})
    ev2 = Evidence(source_api='eidos', text='B',
                   annotations={'found_by': 'dueToSyntax2-Causal'})
    ev3 = Evidence(source_api='hume', text='C')
    ev4 = Evidence(source_api='cwms', text='D')
    ev5 = Evidence(source_api='sofia', text='E')
    ev6 = Evidence(source_api='sofia', text='F')
    x = Event(Concept('x', db_refs={'TEXT': 'dog'}))
    y = Event(Concept('y', db_refs={'TEXT': 'cat'}))
    stmt1 = Influence(x, y, evidence=[ev1, ev2])
    stmt2 = Influence(x, y, evidence=[ev1, ev3])
    stmt3 = Influence(x, y, evidence=[ev3, ev4, ev5])
    stmt4 = Influence(x, y, evidence=[ev5])
    stmt5 = Influence(x, y, evidence=[ev6])
    stmt1.uuid = '1'
    stmt2.uuid = '2'
    stmt3.uuid = '3'
    stmt4.uuid = '4'
    stmt5.uuid = '5'
    stmts = [stmt1, stmt2, stmt3, stmt4]
    return stmts


wm_raw_stmts = _make_wm_stmts()
