import logging
import requests
from collections import defaultdict
from indra.config import get_config
from indra.pipeline import register_pipeline
from indra.ontology.ontology_graph import IndraOntology, with_initialize


logger = logging.getLogger(__name__)

flat_onto_url = ('https://raw.githubusercontent.com/WorldModelers/'
                 'Ontologies/master/wm_flat_metadata.yml')

comp_onto_url = ('https://raw.githubusercontent.com/WorldModelers/Ontologies/'
                 'master/CompositionalOntology_metadata.yml')


def get_term(node, prefix):
    node = node.replace(' ', '_')
    path = prefix + '/' + node if prefix else node
    return WorldOntology.label('WM', path)


def load_yaml_from_path(path):
    """Return a YAML object loaded from a YAML file URL."""
    import yaml
    if path.startswith('http'):
        res = requests.get(path)
        res.raise_for_status()
        yml_str = res.content
    else:
        with open(path, 'r') as fh:
            yml_str = fh.read()
    root = yaml.load(yml_str, Loader=yaml.FullLoader)
    return root


class WorldOntology(IndraOntology):
    """Represents the ontology used for World Modelers applications.

    Parameters
    ----------
    url : str
        The URL or file path pointing to a World Modelers ontology YAML.

    Attributes
    ----------
    url : str
        The URL or file path pointing to a World Modelers ontology YAML.
    yml : list
        The ontology YAML as loaded by the yaml package from the
        URL.
    """
    name = 'world'
    version = '1.0'

    def __init__(self, url):
        super().__init__()
        self.yml = None
        self.url = url

    def initialize(self):
        """Load the World Modelers ontology from the web and build the
        graph."""
        logger.info('Initializing world ontology from %s' % self.url)
        self.add_wm_ontology(self.url)
        self._initialized = True
        self._build_transitive_closure()
        logger.info('Ontology has %d nodes' % len(self))

    def add_wm_ontology(self, url):
        self.yml = load_yaml_from_path(url)
        self._load_yml(self.yml)

    @with_initialize
    def dump_yml_str(self):
        """Return a string-serialized form of the loaded YAML

        Returns
        -------
        str
            The YAML string of the ontology.
        """
        import yaml
        return yaml.dump(self.yml)

    def _load_yml(self, yml):
        self.clear()
        if isinstance(yml, dict) and set(yml) == {'node'}:
            node = yml['node']
            self.build_relations_new_format(node, prefix='')
        else:
            for top_entry in yml:
                node = list(top_entry.keys())[0]
                self.build_relations(node, top_entry[node], None)

    def build_relations_new_format(self, node, prefix):
        """Build relations for the new ontology format > v3.0"""
        # Put together the ID and attributes for this node
        name = node['name']
        this_prefix = prefix + '/' + name if prefix else name
        node_data = {'name': name}
        attrs = ['polarity', 'semantic type', 'examples']
        for attr in attrs:
            if attr in node:
                node_data[attr] = node[attr]
        node_label = self.label('WM', this_prefix)

        # Now iterate over children nodes and make isa edges
        edges = []
        for child in node.get('children', []):
            self.build_relations_new_format(child['node'], this_prefix)
            child_label = self.label('WM', this_prefix + '/' +
                                     child['node']['name'])
            edges.append((child_label, node_label, {'type': 'isa'}))

        # Add node and edges to graph
        self.add_nodes_from([(node_label, node_data)])
        self.add_edges_from(edges)


    def build_relations(self, node, tree, prefix):
        """Build relations for the classic ontology format <= v3.0"""
        nodes = defaultdict(dict)
        edges = []
        this_term = get_term(node, prefix)
        node = node.replace(' ', '_')
        if prefix is not None:
            prefix = prefix.replace(' ', '_')
        this_prefix = prefix + '/' + node if prefix else node
        for entry in tree:
            # This is typically a list of examples which we don't need to
            # independently process
            if isinstance(entry, str):
                continue
            # This is the case of an entry with multiple attributes
            elif isinstance(entry, dict):
                # This is the case of an intermediate node that doesn't have
                # an "OntologyNode" attribute.
                if 'OntologyNode' not in entry:
                    # We take all its children and process them if they are
                    # child ontology entries
                    for child in entry:
                        if child[0] != '_' and child != 'examples' \
                                and isinstance(entry[child], (list, dict)):
                            self.build_relations(child, entry[child],
                                                 this_prefix)
                    # This contains information about a non-leaf node
                    # like examples
                    if 'InnerOntologyNode' in entry:
                        child = None
                # Otherwise this is a leaf term
                else:
                    child = entry['name']
            # This is the case of an intermediate ontology node
            if child is None:
                # Handle opposite entries
                opp = entry.get('opposite')
                if opp:
                    parts = opp.split('/')
                    opp_term = get_term(parts[-1], '/'.join(parts[:-1]))
                    edges.append((opp_term, this_term,
                                  {'type': 'is_opposite'}))
                    edges.append((this_term, opp_term,
                                  {'type': 'is_opposite'}))
                # Handle polarity
                pol = entry.get('polarity')
                if pol is not None:
                    nodes[this_term]['polarity'] = pol
            # This is the case of a leaf term
            elif child[0] != '_' and child != 'examples':
                # Add parenthood relationship
                child_term = get_term(child, this_prefix)
                edges.append((child_term, this_term, {'type': 'isa'}))
                # Handle opposite entries
                opp = entry.get('opposite')
                if opp:
                    parts = opp.split('/')
                    opp_term = get_term(parts[-1], '/'.join(parts[:-1]))
                    edges.append((opp_term, child_term,
                                  {'type': 'is_opposite'}))
                    edges.append((child_term, opp_term,
                                  {'type': 'is_opposite'}))
                # Handle polarity
                pol = entry.get('polarity')
                if pol is not None:
                    nodes[child_term]['polarity'] = pol
        # Now add all the nodes and edges
        self.add_nodes_from([(k, v) for k, v in dict(nodes).items()])
        self.add_edges_from(edges)

    @with_initialize
    def add_entry(self, entry, examples=None, neg_examples=None):
        """Add a new ontology entry with examples.

        This works by adding the entry to the yml attribute first
        and then reloading the entire yaml to build a new graph.

        Parameters
        ----------
        entry : str
            The new entry.
        examples : Optional[list of str]
            Examples for the new entry.
        neg_examples : Optional[list of str]
            Negative examples for the new entry.
        """
        examples = examples if examples else []
        neg_examples = neg_examples if neg_examples else []
        parts = entry.split('/')
        # We start at the root of the YML tree and walk down from
        # there
        root = self.yml
        # We iterate over all the parts of the new grounding entry
        for idx, part in enumerate(parts):
            last_part = (idx == (len(parts) - 1))
            matched_node = None
            # We now look at all the elements of the existing ontology
            # subtree to see if any of them match the new entry.
            for element in root:
                # If this is an OntologyNode
                if 'OntologyNode' in element:
                    if element['name'] == part:
                        matched_node = element
                        break
                # Otherwise, this is an intermediate node
                else:
                    assert len(element) == 1
                    key = list(element.keys())[0]
                    if key == part:
                        matched_node = element[key]
                        break
            # If we matched an existing node and this is the last part
            # then we just need to add the given example
            if matched_node:
                # NOTE: we have to check for 'OntologyNode' in matched_node here
                # because intermediate nodes don't have properties so it's
                # not possible to set examples for them.
                if last_part and 'OntologyNode' in matched_node:
                    matched_node['examples'] += examples
                    if neg_examples:
                        if 'neg_examples' in matched_node:
                            matched_node['neg_examples'] += neg_examples
                        else:
                            matched_node['neg_examples'] = neg_examples
                elif last_part and isinstance(matched_node, list) and \
                        'InnerOntologyNode' in matched_node[0]:
                    matched_node = matched_node[0]
                    matched_node['examples'] += examples
                    if 'neg_examples' in matched_node:
                        matched_node['neg_examples'] += neg_examples
                    else:
                        matched_node['neg_examples'] = neg_examples
            # If we didn't match an existing node, we have to build up
            # a new subtree starting from the current part
            else:
                if last_part:
                    new_entry = {'OntologyNode': None, 'name': part,
                                 'examples': examples}
                    if neg_examples:
                        new_entry['neg_examples'] = neg_examples
                    root.append(new_entry)
                    break
                else:
                    root.append({part: []})
                    matched_node = root[-1][part]
            root = matched_node
        self._load_yml(self.yml)


@register_pipeline
def load_world_ontology(url=None, default_type='compositional'):
    """Load the world ontology from a given URL or file path."""
    conf_url = get_config('INDRA_WORLD_ONTOLOGY_URL')
    if url:
        use_url = url
    elif conf_url:
        use_url = conf_url
    elif default_type == 'flat':
        use_url = flat_onto_url
    else:
        use_url = comp_onto_url
    return WorldOntology(use_url)


world_ontology = load_world_ontology()
