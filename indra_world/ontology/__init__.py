"""Module containing the implementation of an IndraOntology for the
World Modelers use case. """
from .ontology import world_ontology, wm_ont_url, load_world_ontology, \
    WorldOntology


comp_onto_branch = '4531c084d3b902f04605c11396a25db4fff16573'
comp_ontology_url = 'https://raw.githubusercontent.com/WorldModelers/' \
                    'Ontologies/%s/CompositionalOntology_v2.1_metadata.yml' % \
                    comp_onto_branch
comp_ontology = WorldOntology(comp_ontology_url)
