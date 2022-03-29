import copy
import tqdm
import logging
from copy import deepcopy
import networkx
from collections import defaultdict
from indra.pipeline import AssemblyPipeline
from indra.belief import extend_refinements_graph
from indra.preassembler.refinement import RefinementConfirmationFilter
from indra_world.ontology import world_ontology
from indra_world.belief import get_eidos_scorer
from indra_world.assembly.operations import CompositionalRefinementFilter
from indra_world.assembly.operations import \
    location_matches_compositional, location_refinement_compositional, \
    add_flattened_grounding_compositional, standardize_names_compositional


logger = logging.getLogger(__name__)

# TODO: should we use the Bayesian scorer?
eidos_scorer = get_eidos_scorer()


class IncrementalAssembler:
    """Assemble a set of prepared statements and allow incremental extensions.

    Parameters
    ----------
    prepared_stmts : list[indra.statements.Statement]
        A list of prepared INDRA Statements.
    refinement_filters : Optional[list[indra.preassembler.refinement.RefinementFilter]]
        A list of refinement filter classes to be used for refinement
        finding. Default: the standard set of compositional refinement filters.
    matches_fun : Optional[function]
        A custom matches function for determining matching statements and
        calculating hashes. Default: matches function that takes
        compositional grounding and location into account.
    curations : dict[dict]
        A dict of user curations to be integrated into the assembly results,
        keyed by statement hash.
    post_processing_steps : list[dict]
        Steps that can be used in an INDRA AssemblyPipeline to do
        post-processing on statements.

    Attributes
    ----------
    refinement_edges : set
        A set of tuples of statement hashes representing refinement links
        between statements.
    """
    def __init__(self, prepared_stmts,
                 refinement_filters=None,
                 matches_fun=location_matches_compositional,
                 curations=None,
                 post_processing_steps=None,
                 ontology=None):
        self.matches_fun = matches_fun
        # These are preassembly data structures
        self.stmts_by_hash = {}
        self.evs_by_stmt_hash = {}
        self.refinement_edges = set()
        self.prepared_stmts = prepared_stmts
        self.known_corrects = set()
        self.ontology = ontology if ontology else world_ontology

        if not refinement_filters:
            logger.info('Instantiating refinement filters')
            crf = CompositionalRefinementFilter(ontology=self.ontology)
            rcf = RefinementConfirmationFilter(ontology=self.ontology,
                refinement_fun=location_refinement_compositional)
            self.refinement_filters = [crf, rcf]
        else:
            self.refinement_filters = refinement_filters

        self.curations = curations if curations else {}
        self.post_processing_steps = [
                {'function': 'add_flattened_grounding_compositional'},
                {'function': 'standardize_names_compositional'},
            ] \
            if post_processing_steps is None else post_processing_steps

        self.deduplicate()
        self.apply_curations()
        self.get_refinements()
        self.refinements_graph = \
            self.build_refinements_graph(self.stmts_by_hash,
                                         self.refinement_edges)
        self.belief_scorer = eidos_scorer
        self.beliefs = self.get_beliefs()

    def get_curation_effects(self, curations):
        mappings = {}
        for stmt_hash, curation in curations.items():
            new_hash = self.get_curation_effect(stmt_hash, curation)
            if new_hash:
                mappings[stmt_hash] = new_hash
        return mappings

    def get_curation_effect(self, old_hash, curation):
        """Return changed matches hash as a result of curation."""
        relevant_types = {'factor_polarity', 'reverse_relation',
                          'factor_grounding'}
        if curation['update_type'] not in relevant_types:
            return None
        # This should work but we don't want to error in case
        # the hash is missing.
        stmt = self.stmts_by_hash.get(old_hash)
        if not stmt:
            return None
        # Make a deepcopy so we don't persist changes
        stmt = copy.deepcopy(stmt)
        # Flip the polarity
        if curation['update_type'] == 'factor_polarity':
            self.apply_polarity_curation(stmt, curation)
        # Flip subject/object
        elif curation['update_type'] == 'reverse_relation':
            self.apply_reverse_curation(stmt, curation)
        # Change grounding
        elif curation['update_type'] == 'factor_grounding':
            self.apply_grounding_curation(stmt, curation)

        new_hash = stmt.get_hash(matches_fun=self.matches_fun,
                                 refresh=True)
        if new_hash != old_hash:
            return new_hash
        else:
            return None

    @staticmethod
    def apply_polarity_curation(stmt, curation):
        role, new_pol = parse_factor_polarity_curation(curation)
        if role == 'subj':
            stmt.subj.delta.polarity = new_pol
        elif role == 'obj':
            stmt.obj.delta.polarity = new_pol

    @staticmethod
    def apply_reverse_curation(stmt, curation):
        stmt.subj, stmt.obj = stmt.obj, stmt.subj
        # TODO: update evidence annotations

    @staticmethod
    def apply_grounding_curation(stmt, curation):
        role, txt, grounding = parse_factor_grounding_curation(curation)
        # FIXME: It is not clear how compositional groundings will be
        # represented in curations. This implementation assumes a single
        # grounding entry to which we assign a score of 1.0

        # Compositional grounding
        if isinstance(grounding, list):
            grounding_entry = [(gr, 1.0) if gr else None for gr in grounding]
        # Flat grounding
        else:
            grounding_entry = (grounding, 1.0)
        if role == 'subj':
            stmt.subj.concept.db_refs['WM'][0] = grounding_entry
        elif role == 'obj':
            stmt.obj.concept.db_refs['WM'][0] = grounding_entry

    def apply_curations(self):
        """Apply the set of curations to the de-duplicated statements."""
        for stmt_hash, curation in self.curations.items():
            if stmt_hash not in self.stmts_by_hash:
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
                self.apply_polarity_curation(stmt, curation)
            # Flip subject/object
            elif curation['update_type'] == 'reverse_relation':
                self.apply_reverse_curation(stmt, curation)
            # Change grounding
            elif curation['update_type'] == 'factor_grounding':
                self.apply_grounding_curation(stmt, curation)
            else:
                logger.warning('Unknown curation type: %s' %
                               curation['update_type'])

            # We now update statement data structures in case the statement
            # changed in a meaningful way
            if curation['update_type'] in {'factor_polarity',
                                           'reverse_relation',
                                           'factor_grounding'}:
                # First, calculate the new hash
                new_hash = stmt.get_hash(matches_fun=self.matches_fun,
                                         refresh=True)
                # If we don't have a statement yet with this new hash, we
                # move the statement and evidences from the old to the new hash
                if new_hash not in self.stmts_by_hash:
                    self.stmts_by_hash[new_hash] = \
                        self.stmts_by_hash.pop(stmt_hash)
                    self.evs_by_stmt_hash[new_hash] = \
                        self.evs_by_stmt_hash.pop(stmt_hash)
                # If there is already a statement with the new hash, we leave
                # that as is in stmts_by_hash, and then extend evs_by_stmt_hash
                # with the evidences of the curated statement.
                else:
                    self.evs_by_stmt_hash[new_hash] += \
                        self.evs_by_stmt_hash.pop(stmt_hash)

    def deduplicate(self):
        """Build hash-based statement and evidence data structures to
        deduplicate."""
        logger.info('Deduplicating prepared statements')
        for stmt in tqdm.tqdm(self.prepared_stmts):
            self.annotate_evidences(stmt)
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
        """Calculate refinement relationships between de-duplicated statements.
        """
        logger.info('Initializing refinement filters')
        for filter in self.refinement_filters:
            filter.initialize(self.stmts_by_hash)
        logger.info('Applying refinement filters')
        for sh, stmt in tqdm.tqdm(self.stmts_by_hash.items()):
            refinements = None
            for filter in self.refinement_filters:
                # This gets less specific hashes
                refinements = filter.get_related(stmt, refinements)
            # Here we need to add less specific first and more specific second
            refinement_edges = {(ref, sh) for ref in refinements}
            self.refinement_edges |= refinement_edges

    @staticmethod
    def build_refinements_graph(stmts_by_hash, refinement_edges):
        """Return a refinements graph based on statements and refinement edges.
        """
        logger.info('Building refinement graph')
        g = networkx.DiGraph()
        nodes = [(sh, {'stmt': stmt}) for sh, stmt in stmts_by_hash.items()]
        g.add_nodes_from(nodes)
        g.add_edges_from(refinement_edges)
        return g

    def add_statements(self, stmts):
        """Add new statements for incremental assembly.

        Parameters
        ----------
        stmts : list[indra.statements.Statement]
            A list of new prepared statements to be incrementally assembled
            into the set of existing statements.

        Returns
        -------
        AssemblyDelta
            An AssemblyDelta object representing the changes to the assembly
            as a result of the new added statements.
        """
        # We fist organize statements by hash
        stmts_by_hash = defaultdict(list)
        for stmt in stmts:
            self.annotate_evidences(stmt)
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
        # Here we run some post-processing steps on the new statements
        ap = AssemblyPipeline(steps=self.post_processing_steps)
        # NOTE: the assumption here is that the processing steps modify the
        # statement objects directly, this could be modified to return
        # statements that are then set in the hash-keyed dict
        ap.run(list(new_stmts.values()))

        # Next we extend refinements and re-calculate beliefs
        logger.info('Extending refinement filters')
        for filter in self.refinement_filters:
            filter.extend(new_stmts)
        new_refinements = set()
        logger.info('Finding refinements for new statements')
        for sh, stmt in tqdm.tqdm(new_stmts.items()):
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
        logger.info('Getting beliefs')
        beliefs = self.get_beliefs()
        logger.info('Returning assembly delta')
        return AssemblyDelta(new_stmts, new_evidences, new_refinements,
                             beliefs, matches_fun=self.matches_fun)

    def get_all_supporting_evidence(self, sh):
        """Return direct and indirect evidence for a statement hash."""
        all_evs = set(self.evs_by_stmt_hash[sh])
        for supp in networkx.descendants(self.refinements_graph, sh):
            all_evs |= set(self.evs_by_stmt_hash[supp])
        return all_evs

    def get_beliefs(self):
        """Calculate and return beliefs for all statements."""
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
        """Return a flat list of statements with their evidences."""
        stmts = []
        for sh, stmt in deepcopy(self.stmts_by_hash).items():
            stmt.evidence = self.evs_by_stmt_hash.get(sh, [])
            stmt.belief = self.beliefs[sh]
            stmts.append(stmt)
        # TODO: add refinement edges as supports/supported_by?
        # Here we run some post-processing steps on the statements
        ap = AssemblyPipeline(steps=self.post_processing_steps)
        stmts = ap.run(stmts)
        return stmts

    @staticmethod
    def annotate_evidences(stmt):
        """Add annotations to evidences of a given statement."""
        for ev in stmt.evidence:
            raw_text = [None if ag is None else ag.db_refs.get('TEXT')
                        for ag in stmt.agent_list(deep_sorted=True)]
            if 'agents' in ev.annotations:
                ev.annotations['agents']['raw_text'] = raw_text
            else:
                ev.annotations['agents'] = {'raw_text': raw_text}


class AssemblyDelta:
    """Represents changes to the assembly structure as a result of new
    statements added to a set of existing statements.

    Attributes
    ----------
    new_stmts : dict[str, indra.statements.Statement]
        A dict of new statement keyed by hash.
    new_evidences : dict[str, indra.statements.Evidence]
        A dict of new evidences for existing or new statements keyed
        by statement hash.
    new_refinements: list[tuple]
        A list of statement hash pairs representing new refinement links.
    beliefs : dict[str, float]
        A dict of belief scores keyed by all statement hashes (both old and
        new).
    matches_fun : Optional[Callable[[Statement], str]]
        An optional custom matches function. When using a custom matches
        function for assembly, providing it here is necessary to get
        correct JSON serialization.
    """
    def __init__(self, new_stmts, new_evidences, new_refinements, beliefs,
                 matches_fun=None):
        self.new_stmts = new_stmts
        self.new_evidences = new_evidences
        self.new_refinements = new_refinements
        self.beliefs = beliefs
        self.matches_fun = matches_fun

    def to_json(self):
        """Return a JSON representation of the assembly delta."""
        # Serialize statements with custom matches function to make
        # sure matches hashes are consistent
        logger.info('Serializing new statements')
        new_stmts_json = {sh: stmt.to_json(matches_fun=self.matches_fun)
                          for sh, stmt in self.new_stmts.items()}
        logger.info('Serialized %d new statements' % len(new_stmts_json))
        # Pop out evidence since it is redundant with the new_evidence field
        for stmtj in new_stmts_json.values():
            stmtj.pop('evidence', None)
        # Serialize new evidences
        logger.info('Serializing new evidences')
        new_evs_json = {sh: [ev.to_json() for ev in evs]
                        for sh, evs in self.new_evidences.items()}
        logger.info('Serialized new evidences for %d statements' %
                    len(new_evs_json))
        # Return the full construct
        logger.info('Returning with assembly delta JSON')
        return {
            'new_stmts': new_stmts_json,
            'new_evidence': new_evs_json,
            'new_refinements': list(self.new_refinements),
            'beliefs': self.beliefs
        }


def parse_factor_polarity_curation(cur):
    """Parse details from a curation that changes an event's polarity."""
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
    """Parse details from a curation that changes a concept's grounding."""
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
