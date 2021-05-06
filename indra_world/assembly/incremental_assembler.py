from copy import deepcopy
import networkx
from collections import defaultdict
from indra.belief import extend_refinements_graph
from indra.preassembler.refinement import RefinementConfirmationFilter
from indra_world.belief import get_eidos_scorer
from indra_world.ontology import load_world_ontology
from indra_world.assembly.operations import CompositionalRefinementFilter
from indra_world.assembly.operations import \
    location_matches_compositional, location_refinement_compositional


comp_onto_url = 'https://raw.githubusercontent.com/WorldModelers/Ontologies/' \
                'master/CompositionalOntology_v2.1_metadata.yml'

world_ontology = load_world_ontology(comp_onto_url)
# TODO: should we use the Bayesian scorer?
eidos_scorer = get_eidos_scorer()


class IncrementalAssembler:
    def __init__(self, prepared_stmts,
                 refinement_filters=None,
                 matches_fun=location_matches_compositional,
                 curations=None):
        self.matches_fun = matches_fun
        # These are preassembly data structures
        self.stmts_by_hash = {}
        self.evs_by_stmt_hash = {}
        self.refinement_edges = set()
        self.prepared_stmts = prepared_stmts
        self.known_corrects = set()

        if not refinement_filters:
            crf = CompositionalRefinementFilter(ontology=world_ontology)
            rcf = RefinementConfirmationFilter(ontology=world_ontology,
                refinement_fun=location_refinement_compositional)
            self.refinement_filters = [crf, rcf]
        else:
            self.refinement_filters = refinement_filters

        self.curations = curations if curations else []

        self.deduplicate()
        self.apply_curations()
        self.get_refinements()
        self.refinements_graph = \
            self.build_refinements_graph(self.stmts_by_hash,
                                         self.refinement_edges)
        self.belief_scorer = eidos_scorer
        self.beliefs = self.get_beliefs()

    def apply_curations(self):
        hashes_by_uuid = {stmt.uuid: sh
                          for sh, stmt in self.stmts_by_hash.items()}
        for curation in self.curations:
            stmt_hash = hashes_by_uuid.get(curation['statement_id'])
            if not stmt_hash:
                continue
            stmt = self.stmts_by_hash[stmt_hash]
            # Remove the statement
            if curation['update_type'] == 'discard_statement':
                self.stmts_by_hash.pop(stmt_hash, None)
                self.evs_by_stmt_hash.pop(stmt_hash, None)
                # TODO: update belief model here
            # Vet the statement
            elif curation['update_type'] == 'vet_statement':
                self.known_corrects.add(stmt_hash)
                # TODO: update belief model here
            # Flip the polarity
            elif curation['update_type'] == 'factor_polarity':
                role, new_pol = parse_factor_polarity_curation(curation)
                if role == 'subj':
                    stmt.subj.delta.polarity = new_pol
                elif role == 'obj':
                    stmt.obj.delta.polarity = new_pol
                else:
                    continue
            # Flip subject/object
            elif curation['update_type'] == 'reverse_relation':
                tmp = stmt.subj
                stmt.subj = stmt.obj
                stmt.obj = tmp
                # TODO: update evidence annotations
            # Change grounding
            elif curation['update_type'] == 'factor_grounding':
                role, txt, grounding = parse_factor_grounding_curation(curation)
                # FIXME: It is not clear how compositional groundings will be
                # represented in curations. This implementation assumes a single
                # grounding entry to which we assign a score of 1.0
                if role == 'subj':
                    stmt.subj.concept.db_refs['WM'][0] = (grounding, 1.0)
                elif role == 'obj':
                    stmt.obj.concept.db_refs['WM'][0] = (grounding, 1.0)

    def deduplicate(self):
        for stmt in self.prepared_stmts:
            stmt_hash = stmt.get_hash(matches_fun=self.matches_fun)
            evs = stmt.evidence
            if stmt_hash not in self.stmts_by_hash:
                # FIXME: this may be enabled since evidences are kept under
                # a separate data structure, however, then tests may need to
                # be updated to work around the fact that statements are
                # modified.
                # stmt.evidence = []
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
                # This gets less specific hashes
                refinements = filter.get_related(stmt, refinements)
            # Here we need to add less specific first and more specific second
            refinement_edges = {(ref, sh) for ref in refinements}
            self.refinement_edges |= refinement_edges

    def build_refinements_graph(self, stmts_by_hash, refinement_edges):
        g = networkx.DiGraph()
        nodes = [(sh, {'stmt': stmt}) for sh, stmt in stmts_by_hash.items()]
        g.add_nodes_from(nodes)
        g.add_edges_from(refinement_edges)
        return g

    def add_statements(self, stmts):
        # We fist organize statements by hash
        stmts_by_hash = defaultdict(list)
        for stmt in stmts:
            stmts_by_hash[
                stmt.get_hash(matches_fun=self.matches_fun)].append(stmt)
        stmts_by_hash = dict(stmts_by_hash)

        # We next create the new statements and new evidences data structures
        new_stmts = {}
        new_evidences = defaultdict(list)
        for sh, stmts_for_hash in stmts_by_hash.items():
            if sh not in self.stmts_by_hash:
                new_stmts[sh] = stmts_for_hash[0]
                self.stmts_by_hash[sh] = stmts_for_hash[0]
                self.evs_by_stmt_hash[sh] = []
            for stmt in stmts_for_hash:
                for ev in stmt.evidence:
                    new_evidences[sh].append(ev)
                    self.evs_by_stmt_hash[sh].append(ev)
        new_evidences = dict(new_evidences)

        # Next we extend refinements and re-calculate beliefs
        for filter in self.refinement_filters:
            filter.extend(new_stmts)
        new_refinements = set()
        for sh, stmt in new_stmts.items():
            refinements = None
            for filter in self.refinement_filters:
                # Note that this gets less specifics
                refinements = filter.get_related(stmt, refinements)
            # We order hashes by less specific first and more specific second
            new_refinements |= {(ref, sh) for ref in refinements}
            # This expects a list of less specific hashes for the statement
            extend_refinements_graph(self.refinements_graph,
                                     stmt, list(refinements),
                                     matches_fun=self.matches_fun)
        beliefs = self.get_beliefs()
        return AssemblyDelta(new_stmts, new_evidences, new_refinements,
                             beliefs)

    def get_all_supporting_evidence(self, sh):
        all_evs = set(self.evs_by_stmt_hash[sh])
        for supp in networkx.descendants(self.refinements_graph, sh):
            all_evs |= set(self.evs_by_stmt_hash[supp])
        return all_evs

    def get_beliefs(self):
        self.beliefs = {}
        for sh, evs in self.evs_by_stmt_hash.items():
            if sh in self.known_corrects:
                self.beliefs[sh] = 1
                # TODO: should we propagate this belief to all the less
                # specific statements? One option is to add those statements'
                # hashes to the known_corrects list and then at this point
                # we won't need any special handling.
            else:
                self.beliefs[sh] = self.belief_scorer.score_evidence_list(
                    self.get_all_supporting_evidence(sh))
        return self.beliefs

    def get_statements(self):
        stmts = []
        for sh, stmt in deepcopy(self.stmts_by_hash).items():
            stmt.evidence = self.evs_by_stmt_hash.get(sh, [])
            stmt.belief = self.beliefs[sh]
            stmts.append(stmt)
        # TODO: add refinement edges as supports/supported_by?
        return stmts


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


def parse_factor_polarity_curation(cur):
    bef_subj = cur['before']['subj']
    bef_obj = cur['before']['obj']
    aft_subj = cur['after']['subj']
    aft_obj = cur['after']['obj']

    if bef_subj['polarity'] != aft_subj['polarity']:
        return 'subj', aft_subj['polarity']
    elif bef_obj['polarity'] != aft_obj['polarity']:
        return 'obj', aft_obj['polarity']
    else:
        return None, None


def parse_factor_grounding_curation(cur):
    bef_subj = cur['before']['subj']
    bef_obj = cur['before']['obj']
    aft_subj = cur['after']['subj']
    aft_obj = cur['after']['obj']

    if bef_subj['concept'] != aft_subj['concept']:
        return 'subj', aft_subj['factor'], aft_subj['concept']
    elif bef_obj['concept'] != aft_obj['concept']:
        return 'obj', aft_obj['factor'], aft_obj['concept']
    else:
        return None, None, None