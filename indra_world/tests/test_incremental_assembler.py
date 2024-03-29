import copy

from indra.statements import Influence, Event, Concept, Evidence
from indra_world.assembly.incremental_assembler import \
    IncrementalAssembler, AssemblyDelta
from indra_world.assembly.operations import location_matches_compositional

e1 = Event(Concept('x',
                   db_refs={'WM': [[('wm/concept/agriculture', 1.0),
                                    None, None, None]],
                            'TEXT': 'some_text1'
                            }))
e2 = Event(Concept('y',
                   db_refs={'WM': [[('wm/concept/agriculture/crop', 1.0),
                                    None, None, None]],
                            'TEXT': 'some_text2'}))
e3 = Event(Concept('z',
                   db_refs={'WM': [[('wm/concept/crisis_or_disaster', 1.0),
                                    None, None, None]]}))
e4 = Event(Concept('a',
                   db_refs={'WM': [[('wm/concept/agriculture/crop/cereals', 1.0),
                                    None, None, None]]}))
ev1 = Evidence('eidos', text='1')
ev2 = Evidence('eidos', text='2')
s1 = Influence(e1, e2, ev1)
s2 = Influence(e2, e2, ev2)
s1h = s1.get_hash(matches_fun=location_matches_compositional)
s2h = s2.get_hash(matches_fun=location_matches_compositional)
assert s1.get_hash() == s1h


def test_assembly_delta_construct_serialize():
    new_stmts = {s1h: s1, s2h: s2}
    new_evidences = {s1h: s1.evidence, s2h: s2.evidence}
    new_refinements = [(s2h, s1h)]
    beliefs = {s1h: 1.0, s2h: 0.5}
    ad = AssemblyDelta(new_stmts=new_stmts,
                       new_evidences=new_evidences,
                       new_refinements=new_refinements,
                       beliefs=beliefs)
    adj = ad.to_json()
    assert 'evidence' not in adj['new_stmts'][s1h]
    assert adj['new_evidence'][s1h]


def test_assembly_delta_custom_matches():
    from indra.statements.context import WorldContext, RefContext
    stmt = copy.deepcopy(s1)
    stmt.subj.context = WorldContext(geo_location=RefContext('Africa'))
    sh = stmt.get_hash(refresh=True,
                       matches_fun=location_matches_compositional)
    new_stmts = {sh: stmt}
    new_evidences = {sh: stmt.evidence}
    new_refinements = []
    beliefs = {sh: 1.0}
    ad = AssemblyDelta(new_stmts, new_evidences, new_refinements, beliefs)
    adj = ad.to_json()
    assert adj['new_stmts'][sh]['matches_hash'] != sh
    ad = AssemblyDelta(new_stmts, new_evidences, new_refinements, beliefs,
                       matches_fun=location_matches_compositional)
    adj = ad.to_json()
    assert adj['new_stmts'][sh]['matches_hash'] == sh


def test_incremental_assembler_constructor():
    ia = IncrementalAssembler([s1, s2])
    assert ia.prepared_stmts == [s1, s2]
    assert ia.stmts_by_hash == {s1h: s1, s2h: s2}
    assert ia.evs_by_stmt_hash == {s1h: [ev1], s2h: [ev2]}, ia.evs_by_stmt_hash
    assert ia.refinement_edges == {(s1h, s2h)}
    assert set(ia.refinements_graph.nodes()) == {s1h, s2h}
    assert set(ia.get_all_supporting_evidence(s1h)) == {ev1, ev2}
    assert set(ia.get_all_supporting_evidence(s2h)) == {ev2}


def test_incremental_assembler_add_statement_new():
    ev3 = Evidence('eidos', text='3')
    s3 = Influence(e1, e3, ev3)
    s3h = s3.get_hash(matches_fun=location_matches_compositional)
    ia = IncrementalAssembler([s1, s2])
    assert ia.evs_by_stmt_hash == {s1h: [ev1], s2h: [ev2]}, ia.evs_by_stmt_hash
    delta = ia.add_statements([s3])
    assert ia.evs_by_stmt_hash == {s1h: [ev1], s2h: [ev2],
                                   s3h: [ev3]}, ia.evs_by_stmt_hash
    assert delta.new_stmts == {s3h: s3}, delta.new_stmts
    assert delta.new_evidences == {s3h: [ev3]}, delta.new_evidences
    assert not delta.new_refinements, delta.new_refinements
    # TODO: test beliefs
    assert set(ia.get_all_supporting_evidence(s1h)) == {ev1, ev2}
    assert set(ia.get_all_supporting_evidence(s2h)) == {ev2}
    assert set(ia.get_all_supporting_evidence(s3h)) == {ev3}


def test_incremental_assembler_add_statement_duplicate():
    ev3 = Evidence('eidos', text='3')
    s3 = Influence(e1, e2, ev3)
    s3h = s3.get_hash(matches_fun=location_matches_compositional)
    ia = IncrementalAssembler([s1, s2])
    delta = ia.add_statements([s3])
    assert not delta.new_stmts, delta.new_stmts
    assert delta.new_evidences == {s3h: [ev3]}, delta.new_evidences
    assert not delta.new_refinements, delta.new_refinements
    # TODO: test beliefs
    assert set(ia.get_all_supporting_evidence(s1h)) == {ev1, ev2, ev3}
    assert set(ia.get_all_supporting_evidence(s2h)) == {ev2}


def test_incremental_assembler_add_statement_new_refinement():
    ev4 = Evidence('eidos', text='4')
    s4 = Influence(e2, e4, ev4)
    s4h = s4.get_hash(matches_fun=location_matches_compositional)
    ia = IncrementalAssembler([s1, s2])
    delta = ia.add_statements([s4])
    assert delta.new_stmts, {s4h: s4}
    assert delta.new_evidences == {s4h: [ev4]}, delta.new_evidences
    assert delta.new_refinements == {(s1h, s4h), (s2h, s4h)}, \
        delta.new_refinements
    # TODO: test beliefs
    assert set(ia.get_all_supporting_evidence(s1h)) == {ev1, ev2, ev4}
    assert set(ia.get_all_supporting_evidence(s2h)) == {ev2, ev4}
    assert set(ia.get_all_supporting_evidence(s4h)) == {ev4}


def test_post_processing_all_stmts():
    stmts = copy.deepcopy([s1, s2])
    ia = IncrementalAssembler(stmts)
    stmts_out = ia.get_statements()
    # Check that we normalized concept names
    assert stmts_out[0].subj.concept.name == 'agriculture'
    # Check that we added flattened groundings
    flat_grounding = [{'grounding': 'wm/concept/agriculture',
                       'name': 'agriculture', 'score': 1.0}]
    assert stmts_out[0].subj.concept.db_refs['WM_FLAT'] == \
        flat_grounding, flat_grounding
    # Check that we added annotations
    assert 'agents' in stmts_out[0].evidence[0].annotations
    assert stmts_out[0].evidence[0].annotations['agents'] == {
        'raw_text': ['some_text1', 'some_text2']
    }, stmts_out[0].evidence[0].annotations['agents']


def test_post_processing_new_stmts():
    stmts = copy.deepcopy([s1, s2])
    ia = IncrementalAssembler([stmts[0]])
    delta = ia.add_statements([stmts[1]])
    assert len(delta.new_stmts) == 1
    stmt = list(delta.new_stmts.values())[0]
    assert stmt.subj.concept.name == 'crop'

    # Check that we added annotations
    assert 'agents' in stmt.evidence[0].annotations
    assert stmt.evidence[0].annotations['agents'] == {
        'raw_text': ['some_text2', 'some_text2']
    }, stmt.evidence[0].annotations['agents']


def test_apply_grounding_curation():
    gr1 = [('theme1', 0.8), None, ('process', 0.7), None]
    gr2 = ['theme2', 'property2', None, None]
    cur = {
        "before": {"subj": {"factor": 'x',
                            "concept": gr1},
                   "obj": {"factor": 'y',
                           "concept": 'z'}},
        "after": {"subj": {"factor": 'x',
                           "concept": gr2},
                  "obj": {"factor": 'y',
                          "concept": 'z'}},
    }
    c1 = Concept('x', db_refs={'WM': [gr1]})
    stmt = Influence(Event(c1), Event('y'))
    IncrementalAssembler.apply_grounding_curation(stmt, cur)
    assert stmt.subj.concept.db_refs['WM'][0] == \
        [('theme2', 1.0), ('property2', 1.0), None, None]