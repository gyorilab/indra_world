import os
import json
from copy import deepcopy

import indra.tools.assemble_corpus as ac
from indra.statements import Concept, Event, Influence
from indra.statements.concept import get_top_compositional_grounding
from indra.pipeline import AssemblyPipeline
from indra.statements import stmts_from_json_file
from indra.statements import Evidence, QualitativeDelta, RefContext, \
    WorldContext
from indra.preassembler import Preassembler

from indra_world.assembly.operations import *
from indra_world.ontology import comp_ontology
from indra_world.ontology import world_ontology
from indra_world.assembly.matches import location_matches


HERE = os.path.dirname(os.path.abspath(__file__))


def test_get_top_compositional_grounding():
    gr1 = [('x', 0.7), None, None, None]
    assert get_top_compositional_grounding([gr1]) == gr1
    gr2 = [('y', 0.6), None, None, None]
    assert get_top_compositional_grounding([gr1, gr2]) == gr1
    assert get_top_compositional_grounding([gr2, gr1]) == gr1
    gr3 = [('z', 0.6), None, ('a', 0.5)]
    assert get_top_compositional_grounding([gr1, gr3]) == gr1
    assert get_top_compositional_grounding([gr2, gr3]) == gr3
    gr4 = [('z', 0.6), None, ('a', 0.4)]
    assert get_top_compositional_grounding([gr4, gr3]) == gr3


def test_compositional_grounding_filter():
    # Test property filtered out based on score
    wm = [[('x', 0.5), ('y', 0.8), None, None]]
    concept = Concept('x', db_refs={'WM': wm})
    stmt = Event(concept)
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    concept = stmt_out.concept
    assert concept.db_refs['WM'][0][0] == ('y', 0.8), concept.db_refs
    assert concept.db_refs['WM'][0][1] is None

    # Test property being promoted to theme
    wm = [[None, ('y', 0.8), None, None]]
    concept.db_refs['WM'] = wm
    stmt = Event(concept)
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    concept = stmt_out.concept
    assert concept.db_refs['WM'][0][0] == ('y', 0.8), concept.db_refs
    assert concept.db_refs['WM'][0][1] is None

    # Test score threshold being equal to score
    wm = [[('x', 0.7), ('y', 0.7), None, None]]
    concept.db_refs['WM'] = wm
    stmt = Event(concept)
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7, [])
    concept = stmt_out.concept
    assert concept.db_refs['WM'][0][0] == ('x', 0.7), concept.db_refs
    assert concept.db_refs['WM'][0][1] == ('y', 0.7), concept.db_refs

    # Score filter combined with groundings to exclude plus promoting
    # a property to a theme
    wm = [[('wm_compositional/entity/geo-location', 0.7), ('y', 0.7),
           None, None]]
    concept.db_refs['WM'] = wm
    stmt = Event(concept)
    stmt_out = compositional_grounding_filter_stmt(stmt, 0.7,
        ['wm_compositional/entity/geo-location'])
    concept = stmt_out.concept
    assert concept.db_refs['WM'][0][0] == ('y', 0.7), concept.db_refs


def test_compositional_refinements():
    def make_event(comp_grounding):
        scored_grounding = [
            (gr, 1.0) if gr else None
            for gr in comp_grounding
        ]
        name = '_'.join([gr.split('/')[-1]
                         for gr in comp_grounding if gr])
        concept = Concept(name=name,
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

    assert compositional_refinement(events[0], events[1],
                                    ontology=comp_ontology,
                                    entities_refined=False)

    assert compositional_refinement(events[3], events[1],
                                    ontology=comp_ontology,
                                    entities_refined=False)

    # Check refinements over events
    filters = [CompositionalRefinementFilter(ontology=comp_ontology)]
    assembled_stmts = \
        ac.run_preassembly(events,
                           filters=filters,
                           ontology=comp_ontology,
                           refinement_fun=compositional_refinement,
                           matches_fun=matches_compositional,
                           return_toplevel=False)
    assert len(assembled_stmts) == 4

    refinements = set()
    for stmt in assembled_stmts:
        for supp in stmt.supports:
            refinements.add((stmt.concept.name, supp.concept.name))
        for supp_by in stmt.supported_by:
            refinements.add((supp_by.concept.name, stmt.concept.name))

    refinements = sorted(refinements)
    assert refinements == \
           [('agriculture', 'agriculture_price_or_cost'),
            ('agriculture', 'crop'),
            ('agriculture', 'crop_price_or_cost'),
            ('agriculture_price_or_cost', 'crop_price_or_cost'),
            ('crop', 'crop_price_or_cost')], refinements

    # Check refinements over influences
    influences = [
        Influence(events[0], events[1]),
        Influence(events[0], events[2]),
        Influence(events[3], events[1]),
        Influence(events[3], events[2]),
    ]
    filters = [CompositionalRefinementFilter(ontology=comp_ontology)]
    assembled_stmts = \
        ac.run_preassembly(influences,
                           filters=filters,
                           ontology=comp_ontology,
                           refinement_fun=compositional_refinement,
                           matches_fun=matches_compositional,
                           return_toplevel=False)
    assert len(assembled_stmts) == 4

    refinements = set()
    for stmt in assembled_stmts:
        for supp in stmt.supports:
            refinements.add((stmt.subj.concept.name,
                             stmt.obj.concept.name,
                             supp.subj.concept.name,
                             supp.obj.concept.name))
        for supp_by in stmt.supported_by:
            refinements.add((supp_by.subj.concept.name,
                             supp_by.obj.concept.name,
                             stmt.subj.concept.name,
                             stmt.obj.concept.name))

    refinements = sorted(refinements)
    assert refinements == \
           [('agriculture_price_or_cost', 'agriculture',
             'agriculture_price_or_cost', 'crop'),
            ('agriculture_price_or_cost', 'agriculture',
             'crop_price_or_cost', 'agriculture'),
            ('agriculture_price_or_cost', 'agriculture',
             'crop_price_or_cost', 'crop'),
            ('agriculture_price_or_cost', 'crop',
             'crop_price_or_cost', 'crop'),
            ('crop_price_or_cost', 'agriculture',
             'crop_price_or_cost', 'crop')]


comp_assembly_json = [{
    "function": "run_preassembly",
    "kwargs": {
      "filters": {
         "function": "listify",
         "kwargs": {
             "obj": {
                 "function": "make_default_compositional_refinement_filter"
             }
         }
      },
      "belief_scorer": {
        "function": "get_eidos_scorer"
      },
      "matches_fun": {
        "function": "location_matches_compositional",
        "no_run": True
      },
      "refinement_fun": {
        "function": "location_refinement_compositional",
        "no_run": True
      },
      "ontology": {
        "function": "load_world_ontology",
        "kwargs": {
          "url": "https://raw.githubusercontent.com/WorldModelers/Ontologies/4531c084d3b902f04605c11396a25db4fff16573/CompositionalOntology_v2.1_metadata.yml"
        }
      },
      "return_toplevel": False,
      "poolsize": None,
      "run_refinement": True
    }
  }]


def test_assembly_cycle():
    stmts = stmts_from_json_file(
        os.path.join(HERE, 'data', 'compositional_refinement_cycle_test.json'))
    # 874 is a refinement of -534
    pipeline = AssemblyPipeline(comp_assembly_json)
    assembled_stmts = pipeline.run(stmts)
    assert assembled_stmts[0].supported_by == [assembled_stmts[1]]


def test_compositional_refinement_polarity_bug():
    stmts = stmts_from_json_file(
        os.path.join(HERE, 'data', 'test_missing_refinement.json'))
    pipeline = AssemblyPipeline(comp_assembly_json)
    assembled_stmts = pipeline.run(stmts)
    assert assembled_stmts[0].supported_by == [assembled_stmts[1]]


def _get_extended_wm_hierarchy():
    wo = deepcopy(world_ontology)
    wo.initialize()
    wo.add_edge(
        'WM:wm/x/y/z/flooding',
        'WM:wm/a/b/c/flooding',
        **{'type': 'is_equal'}
    )
    wo.add_edge(
        'WM:wm/a/b/c/flooding',
        'WM:wm/x/y/z/flooding',
        **{'type': 'is_equal'}
    )
    return wo


def test_run_preassembly_concepts():
    ont = _get_extended_wm_hierarchy()
    rainfall = Event(Concept('rain', db_refs={
        'WM': ('wm/concept/causal_factor/environmental/meteorologic/'
               'precipitation/rainfall')}))
    flooding_1 = Event(Concept('flood', db_refs={
        'WM': 'wm/x/y/z/flooding'}))
    flooding_2 = Event(Concept('flooding', db_refs={
        'WM': 'wm/a/b/c/flooding'}))
    st_out = ac.run_preassembly([
        Influence(rainfall, flooding_1), Influence(rainfall, flooding_2)],
        normalize_ns='WM', normalize_equivalences=True,
        ontology=ont)
    assert len(st_out) == 1, st_out


def test_merge_deltas():
    def add_annots(stmt):
        for ev in stmt.evidence:
            ev.annotations['subj_adjectives'] = stmt.subj.delta.adjectives
            ev.annotations['obj_adjectives'] = stmt.obj.delta.adjectives
            ev.annotations['subj_polarity'] = stmt.subj.delta.polarity
            ev.annotations['obj_polarity'] = stmt.obj.delta.polarity
        return stmt
    # d1 = {'adjectives': ['a', 'b', 'c'], 'polarity': 1}
    # d2 = {'adjectives': [], 'polarity': -1}
    # d3 = {'adjectives': ['g'], 'polarity': 1}
    # d4 = {'adjectives': ['d', 'e', 'f'], 'polarity': -1}
    # d5 = {'adjectives': ['d'], 'polarity': None}
    # d6 = {'adjectives': [], 'polarity': None}
    # d7 = {'adjectives': [], 'polarity': 1}

    d1 = QualitativeDelta(polarity=1, adjectives=['a', 'b', 'c'])
    d2 = QualitativeDelta(polarity=-1, adjectives=None)
    d3 = QualitativeDelta(polarity=1, adjectives=['g'])
    d4 = QualitativeDelta(polarity=-1, adjectives=['d', 'e', 'f'])
    d5 = QualitativeDelta(polarity=None, adjectives=['d'])
    d6 = QualitativeDelta(polarity=None, adjectives=None)
    d7 = QualitativeDelta(polarity=1, adjectives=None)

    def make_ev(name, delta):
        return Event(Concept(name), delta=delta)

    stmts = [add_annots(Influence(make_ev('a', sd), make_ev('b', od),
                                  evidence=[Evidence(source_api='eidos',
                                                     text='%d' % idx)]))
             for idx, (sd, od) in enumerate([(d1, d2), (d3, d4)])]
    stmts = ac.run_preassembly(stmts, return_toplevel=True)
    stmts = ac.merge_deltas(stmts)
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
    stmts = ac.run_preassembly(stmts, return_toplevel=True)
    stmts = merge_deltas(stmts)
    assert stmts[0].subj.delta.polarity is None, stmts[0].subj.delta
    assert stmts[0].obj.delta.polarity == 1, stmts[0].obj.delta
    assert set(stmts[0].subj.delta.adjectives) == {'a', 'b', 'c'}, \
        stmts[0].subj.delta
    assert set(stmts[0].obj.delta.adjectives) == {'d'}, \
        stmts[0].obj.delta


def test_normalize_equals_opposites():
    ont = _get_extended_wm_hierarchy()
    flooding1 = 'wm/a/b/c/flooding'
    flooding2 = 'wm/x/y/z/flooding'
    # Note that as of 5/15/2020 food_insecurity and food_security aren't
    # explicitly opposites in the ontology
    food_insec = 'wm/concept/causal_factor/food_insecurity/food_nonaccess'
    food_sec = 'wm/concept/causal_factor/food_security/food_access'

    # Top grounding: flooding1
    dbr = {'WM': [(flooding1, 1.0), (flooding2, 0.5), (food_insec, 0.1)]}
    ev1 = Event(Concept('x', db_refs=dbr))

    # Top grounding: food security
    dbr = {'WM': [(food_sec, 1.0), (flooding2, 0.5)]}
    ev2 = Event(Concept('x', db_refs=dbr),
                delta=QualitativeDelta(polarity=1))

    # Make sure that by default, things don't get normalized out
    stmts = ac.run_preassembly([ev1, ev2], ontology=ont)
    assert stmts[0].concept.db_refs['WM'][0][0] != \
           stmts[0].concept.db_refs['WM'][1][0]

    # Now we turn on equivalence normalization and expect
    # that flooding1 and flooding2 have been normalized out
    # in ev1's db_refs
    stmts = ac.run_preassembly([ev1, ev2], normalize_equivalences=True,
                               normalize_ns='WM',
                               ontology=ont)
    assert stmts[0].concept.db_refs['WM'][0][0] == \
           stmts[0].concept.db_refs['WM'][1][0], \
        stmts[0].concept.db_refs['WM']

    # Now we turn on opposite normalization and expect that food
    # security and insecurity will get normalized out
    stmts = ac.run_preassembly([ev1, ev2], normalize_equivalences=True,
                               normalize_opposites=True, normalize_ns='WM',
                               ontology=ont)
    assert len(stmts) == 2
    stmts = sorted(stmts, key=lambda x: len(x.concept.db_refs['WM']),
                   reverse=True)
    assert len(stmts[0].concept.db_refs['WM']) == 3, stmts[0].concept.db_refs
    # This is to check that food_insecurity was normalized to food_security
    assert stmts[0].concept.db_refs['WM'][2][0] == \
           stmts[1].concept.db_refs['WM'][0][0], \
        (stmts[0].concept.db_refs['WM'],
         stmts[1].concept.db_refs['WM'])


def test_event_assemble_location():
    rainfall = Concept('rainfall')
    loc1 = RefContext(name='x', db_refs={'GEOID': '1'})
    loc2 = RefContext(name='x', db_refs={'GEOID': '2'})
    ev1 = Event(rainfall, context=WorldContext(geo_location=loc1))
    ev2 = Event(rainfall, context=WorldContext(geo_location=loc2))

    pa = Preassembler(ontology=world_ontology, stmts=[ev1, ev2],
                      matches_fun=None)
    unique_stmts = pa.combine_duplicates()

    assert len(unique_stmts) == 1
    pa = Preassembler(ontology=world_ontology, stmts=[ev1, ev2],
                      matches_fun=location_matches)
    unique_stmts = pa.combine_duplicates()
    assert len(unique_stmts) == 2


def test_influence_event_hash_reference():
    rainfall = Concept('rainfall')
    loc1 = RefContext(name='x', db_refs={'GEOID': '1'})
    loc2 = RefContext(name='x', db_refs={'GEOID': '2'})
    ev1 = Event(rainfall, context=WorldContext(geo_location=loc1))
    ev2 = Event(rainfall, context=WorldContext(geo_location=loc2))
    infl = Influence(ev1, ev2)

    h1 = ev1.get_hash(refresh=True)
    h2 = ev2.get_hash(refresh=True)
    hl1 = ev1.get_hash(refresh=True, matches_fun=location_matches)
    hl2 = ev2.get_hash(refresh=True, matches_fun=location_matches)

    assert h1 == h2, (h1, h2)
    assert hl1 != hl2, (hl1, hl2)

    ij = infl.to_json(matches_fun=location_matches)
    ev1j = ev1.to_json(matches_fun=location_matches)
    assert ev1j['matches_hash'] == ij['subj']['matches_hash'], \
        (print(json.dumps(ev1j, indent=1)),
         print(json.dumps(ij, indent=1)))


