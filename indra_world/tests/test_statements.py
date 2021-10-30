from indra.statements import Concept, QualitativeDelta, Association, Event
from indra_world.ontology import load_world_ontology

world_ontology = load_world_ontology(default_type='flat',
    url='https://raw.githubusercontent.com/WorldModelers/Ontologies/master/wm_flat_metadata.yml')


def test_concept_isa_eid():
    c1 = Concept('b', db_refs={'WM': [('wm/concept/entity/organization', 1.0)]})
    c2 = Concept('a', db_refs={'WM': [('wm/concept/entity', 1.0)]})
    print(c1.get_grounding())
    print(c2.get_grounding())
    assert c1.refinement_of(c2, world_ontology)
    assert not c2.refinement_of(c1, world_ontology)


def test_concept_opposite_eid():
    a = 'wm/concept/causal_factor/food_security/food_availability'
    b = 'wm/concept/causal_factor/food_insecurity/food_unavailability'
    c1 = Concept('a', db_refs={'WM': [(a, 1.0)]})
    c2 = Concept('b', db_refs={'WM': [(b, 1.0)]})
    assert c1.is_opposite(c2, world_ontology)
    assert c2.is_opposite(c1, world_ontology)


def test_association_contradicts():
    neg = 'wm/concept/causal_factor/food_insecurity/food_unavailability'
    pos = 'wm/concept/causal_factor/food_security/food_availability'
    food_avail_neg = Event(Concept('food security',
                                   db_refs={'WM': pos}),
                           delta=QualitativeDelta(polarity=-1))
    food_avail_pos = Event(Concept('food security',
                                   db_refs={'WM': pos}),
                           delta=QualitativeDelta(polarity=1))
    food_unavail = Event(Concept('food insecurity',
                                 db_refs={'WM': neg}),
                         delta=QualitativeDelta(polarity=1))
    prp = Event(Concept('production'), delta=QualitativeDelta(polarity=1))
    prn = Event(Concept('production'), delta=QualitativeDelta(polarity=-1))

    assert Association([food_avail_neg, prp]).contradicts(
        Association([food_unavail, prn]), world_ontology)
    assert Association([food_avail_neg, prp]).contradicts(
        Association([food_avail_neg, prn]), world_ontology)
    assert Association([prp, food_avail_neg]).contradicts(
        Association([food_avail_neg, prn]), world_ontology)
    assert Association([prn, food_avail_neg]).contradicts(
        Association([food_avail_pos, prn]), world_ontology)
    assert Association([food_avail_neg, food_avail_pos]).contradicts(
        Association([food_unavail, food_avail_neg]), world_ontology)
    assert Association([food_unavail, food_avail_pos]).contradicts(
        Association([food_avail_pos, food_avail_pos]), world_ontology)
    assert Association([food_unavail, food_avail_pos]).contradicts(
        Association([food_avail_neg, food_avail_neg]), world_ontology)
