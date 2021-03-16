from indra.statements import Influence, Event, Concept, Evidence
from indra_wm_service.assembly.incremental_assembler import \
    IncrementalAssembler, AssemblyDelta
from indra_wm_service.assembly.operations import location_matches_compositional

e1 = Event(Concept('x',
                   db_refs={'WM': [[('wm/concept/agriculture', 1.0),
                                    None, None, None]]}))
e2 = Event(Concept('y',
                   db_refs={'WM': [[('wm/concept/agriculture/crop', 1.0),
                                    None, None, None]]}))
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


def test_incremental_assembler_constructor():
    ia = IncrementalAssembler([s1, s2])
    assert ia.prepared_stmts == [s1, s2]
    assert ia.stmts_by_hash == {s1h: s1, s2h: s2}
    assert ia.evs_by_stmt_hash == {s1h: [ev1], s2h: [ev2]}, ia.evs_by_stmt_hash
    assert ia.refinement_edges == {(s2h, s1h)}
    assert set(ia.refinements_graph.nodes()) == {s1h, s2h}


def test_incremental_assembler_add_statement_new():
    ev3 = Evidence('eidos', text='3')
    s3 = Influence(e1, e3, ev3)
    s3h = s3.get_hash(matches_fun=location_matches_compositional)
    ia = IncrementalAssembler([s1, s2])
    delta = ia.add_statements([s3])
    assert delta.new_stmts == {s3h: s3}, delta.new_stmts
    assert delta.new_evidences == {s3h: [ev3]}, delta.new_evidences
    assert not delta.new_refinements, delta.new_refinements
    # TODO: test beliefs


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


def test_incremental_assembler_add_statement_new_refinement():
    ev4 = Evidence('eidos', text='4')
    s4 = Influence(e1, e4, ev4)
    s4h = s4.get_hash(matches_fun=location_matches_compositional)
    ia = IncrementalAssembler([s1, s2])
    delta = ia.add_statements([s4])
    assert delta.new_stmts, {s4h: s4}
    assert delta.new_evidences == {s4h: [ev4]}, delta.new_evidences
    assert delta.new_refinements == {(s4h, s1h), (s4h, s1h)}, \
        delta.new_refinements
    # TODO: test beliefs
