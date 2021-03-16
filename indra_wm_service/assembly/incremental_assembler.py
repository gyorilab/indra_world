import networkx
from collections import defaultdict
from indra.belief import extend_refinements_graph, BeliefEngine
from indra.belief.wm_scorer import get_eidos_scorer
from indra.ontology.world import load_world_ontology
from indra.preassembler.refinement import RefinementConfirmationFilter
from indra_wm_service.assembly.operations import CompositionalRefinementFilter
from indra_wm_service.assembly.operations import \
    location_matches_compositional, location_refinement_compositional


comp_onto_url = 'https://raw.githubusercontent.com/WorldModelers/Ontologies/' \
                'master/CompositionalOntology_v2.1_metadata.yml'

world_ontology = load_world_ontology(comp_onto_url)
# TODO: should we use the Bayesian scorer?
eidos_scorer = get_eidos_scorer()


class IncrementalAssembler:
    def __init__(self, prepared_stmts,
                 matches_fun=location_matches_compositional):
        self.matches_fun = matches_fun
        # These are preassembly data structures
        self.stmts_by_hash = {}
        self.evs_by_stmt_hash = {}
        self.refinement_edges = set()
        self.prepared_stmts = prepared_stmts

        crf = CompositionalRefinementFilter(ontology=world_ontology)
        rcf = RefinementConfirmationFilter(ontology=world_ontology,
            refinement_fun=location_refinement_compositional)
        self.refinement_filters = [crf, rcf]
        self.deduplicate()
        self.get_refinements()
        self.refinements_graph = \
            self.build_refinements_graph(self.stmts_by_hash,
                                         self.refinement_edges)
        self.belief_engine = \
            BeliefEngine(refinements_graph=self.refinements_graph,
                         scorer=eidos_scorer)

    def deduplicate(self):
        for stmt in self.prepared_stmts:
            stmt_hash = stmt.get_hash(matches_fun=self.matches_fun)
            evs = stmt.evidence
            if stmt_hash not in self.stmts_by_hash:
                stmt.evidence = []
                self.stmts_by_hash[stmt_hash] = stmt
            if stmt_hash not in self.evs_by_stmt_hash:
                self.evs_by_stmt_hash[stmt_hash] = []
            self.evs_by_stmt_hash[stmt_hash] += evs

    def get_refinements(self):
        for filter in self.refinement_filters:
            filter.initialize(self.stmts_by_hash)
        for sh, stmt in self.stmts_by_hash.items():
            refinements = None
            for filter in self.refinement_filters:
                refinements = filter.get_related(stmt, refinements)
            refinement_edges = {(sh, ref) for ref in refinements}
            self.refinement_edges |= refinement_edges

    def build_refinements_graph(self, stmts_by_hash, refinement_edges):
        g = networkx.DiGraph()
        nodes = [(sh, {'stmt': stmt})
                 for sh, stmt in stmts_by_hash.items()]
        g.add_nodes_from(nodes)
        g.add_edges_from(refinement_edges)
        return g

    def add_statements(self, stmts):
        # We fist organize statements by hash
        stmts_by_hash = defaultdict(list)
        for stmt in stmts:
            stmts_by_hash[stmt.get_hash(self.matches_fun)].append(stmt)

        # We next create the new statements and new evidences data structures
        new_stmts = {}
        new_evidences = defaultdict(list)
        for sh, stmts_for_hash in stmts_by_hash.items():
            if sh not in self.stmts_by_hash:
                new_stmts[sh] = stmts_for_hash[0]
            for stmt in stmts_for_hash:
                for ev in stmt.evidence:
                    new_evidences[sh].append(ev)

        # Next we extend refinements and re-calculate beliefs
        for filter in self.refinement_filters:
            filter.extend(new_stmts)
        new_refinements = set()
        for sh, stmt in new_stmts.items():
            refinements = None
            for filter in self.refinement_filters:
                refinements = filter.get_related(stmt, refinements)
            new_refinements |= {(sh, ref) for ref in refinements}
            extend_refinements_graph(self.belief_engine.refinements_graph,
                                     stmt, list(refinements),
                                     matches_fun=self.matches_fun)

        beliefs = self.belief_engine.get_hierarchy_probs(
            stmts_by_hash.values())
        return AssemblyDelta(new_stmts, new_evidences, new_refinements,
                             beliefs)


class AssemblyDelta:
    def __init__(self, new_stmts, new_evidences, new_refinements, beliefs):
        self.new_stmts = new_stmts
        self.new_evidences = new_evidences
        self.new_refinements = new_refinements
        self.beliefs = beliefs

    def to_json(self):
        return {
            'new_stmts': {sh: stmt.to_json()
                          for sh, stmt in self.new_stmts.items()},
            'new_evidence': {sh: [ev.to_json() for ev in evs]
                             for sh, evs in self.new_evidences.items()},
            'new_refinements': list(self.new_refinements),
            'beliefs': self.beliefs
        }