from indra.statements import Concept
from indra.statements.concept import get_top_compositional_grounding
from indra_wm_service.assembly.operations import *
from indra.pipeline import AssemblyPipeline
from indra.statements import stmts_from_json_file

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
            "function": "default_refinement_filter_compositional",
            "no_run": True
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
        os.path.join(HERE, 'compositional_refinement_cycle_test.json'))
    # 874 is a refinement of -534
    pipeline = AssemblyPipeline(comp_assembly_json)
    assembled_stmts = pipeline.run(stmts)
    assert assembled_stmts[0].supported_by == [assembled_stmts[1]]


def test_compositional_refinement_polarity_bug():
    stmts = stmts_from_json_file(
        os.path.join(HERE, 'test_missing_refinement.json'))
    stmts_by_hash = {s.get_hash(matches_fun=location_matches_compositional): s
                     for s in stmts}
    refs = default_refinement_filter_compositional(stmts_by_hash, None)
    assert refs[1923264734510249] == {13662095999301093}
    assert not refs[13662095999301093]

    pipeline = AssemblyPipeline(comp_assembly_json)
    assembled_stmts = pipeline.run(stmts)
    assert assembled_stmts[0].supported_by == [assembled_stmts[1]]
