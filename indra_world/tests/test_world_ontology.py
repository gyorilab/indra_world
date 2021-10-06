import copy
import yaml
from indra_world.ontology import load_world_ontology
from indra_world.ontology.ontology import WorldOntology

flat_ontology = load_world_ontology(default_type='flat')


def test_hm_opposite_polarity():
    concept1 = 'wm/concept/causal_factor/food_insecurity/food_instability'
    concept2 = 'wm/concept/causal_factor/food_security/food_stability'
    concept3 = ('wm/concept/causal_factor/environmental/meteorologic/'
                'precipitation/flooding')
    assert flat_ontology.is_opposite('WM', concept1, 'WM', concept2)
    assert flat_ontology.is_opposite('WM', concept2, 'WM', concept1)
    assert not flat_ontology.is_opposite('WM', concept1, 'WM', concept3)
    assert flat_ontology.get_polarity('WM', concept1) == -1
    assert flat_ontology.get_polarity('WM', concept2) == 1
    assert flat_ontology.get_polarity('UN', 'something') is None


def test_world_ontology_add_entry():
    ont = copy.deepcopy(flat_ontology)
    nat_dis = ('wm/concept/causal_factor/crisis_and_disaster/'
               'environmental_disasters/natural_disaster')

    new_node = nat_dis + '/floods'
    assert not ont.isa('WM', new_node, 'WM', nat_dis)
    ont.add_entry(new_node, examples=['floods'])
    assert ont.isa('WM', new_node, 'WM', nat_dis)
    ont_yml = ont.dump_yml_str()


def test_load_intermediate_nodes():
    url = ('https://raw.githubusercontent.com/clulab/eidos/posneg/src/main/'
           'resources/org/clulab/wm/eidos/english/ontologies/'
           'wm_posneg_metadata.yml')
    wo = WorldOntology(url)
    wo.initialize()
    assert wo.is_opposite('WM', 'wm/concept/causal_factor/food_insecurity',
                          'WM', 'wm/concept/causal_factor/food_security')
    assert wo.is_opposite('WM', 'wm/concept/causal_factor/food_security',
                          'WM', 'wm/concept/causal_factor/food_insecurity')

    assert wo.get_polarity('WM',
                           'wm/concept/causal_factor/food_insecurity') == -1
    wo.add_entry('wm/concept', examples=['xxx'], neg_examples=['yyy'])
    entry = wo.yml[0]['wm'][0]['concept'][0]
    assert 'xxx' in entry['examples']
    assert 'yyy' in entry['neg_examples']


def test_new_onto_format():
    ont_yml = """
node:
    name: wm
    children:
        - node:
            name: concept
            children:
                - node:
                    name: agriculture
                    children:
                        - node:
                            name: animal_feed
                            examples:
                                - additives
                                - amounts
                            polarity: 1
                            semantic type: entity
                        - node:
                            name: animal_science
                            examples:
                                - agricultural science
                                - agriculture organization
                                - animal production
                            polarity: 1
                            semantic type: event
    """
    yml = yaml.load(ont_yml, Loader=yaml.FullLoader)
    wo = WorldOntology(None)
    wo._load_yml(yml)
    wo._initialized = True
    assert len(wo.nodes) == 5
    assert len(wo.edges) == 4
    assert all(e['type'] == 'isa' for _, _, e in wo.edges(data=True))
    assert 'WM:wm' in wo, wo.nodes()
    assert wo.nodes['WM:wm/concept/agriculture/animal_feed']['name'] == \
        'animal_feed'
    examples = set(wo.nodes['WM:wm/concept/agriculture/animal_feed']['examples'])
    assert examples == {'additives', 'amounts'}
    assert wo.isa('WM', 'wm/concept', 'WM', 'wm')
