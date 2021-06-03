__all__ = ['compositional_refinement', 'location_refinement_compositional',
           'make_compositional_refinement_filter',
           'make_default_compositional_refinement_filter',
           'get_relevants_for_stmt', 'event_compositional_refinement',
           'event_location_refinement', 'location_refinement',
           'event_location_time_refinement', 'location_time_refinement',
           'event_location_time_delta_refinement',
           'location_time_delta_refinement', 'CompositionalRefinementFilter',
           'get_agent_key']

import collections
from .matches import has_location, has_time, get_location
from indra.statements import Influence, Event
from indra.pipeline import register_pipeline
from indra.preassembler import RefinementFilter, get_relevant_keys
from indra_world.ontology import comp_ontology
from .matches import concept_matches_compositional


@register_pipeline
def compositional_refinement(st1, st2, ontology, entities_refined):
    if type(st1) != type(st2):
        return False
    if isinstance(st1, Event):
        return event_compositional_refinement(st1, st2, ontology,
                                              entities_refined)
    elif isinstance(st1, Influence):
        subj_ref = event_compositional_refinement(st1.subj, st2.subj,
                                                  ontology, entities_refined,
                                                  ignore_polarity=True)
        if not subj_ref:
            return False
        obj_ref = event_compositional_refinement(st1.subj, st2.subj,
                                                 ontology, entities_refined,
                                                 ignore_polarity=True)
        if not obj_ref:
            return False
        delta_refinement = st1.delta_refinement_of(st2)
        if delta_refinement:
            return True
        else:
            return False
    # TODO: handle Associations?
    return False


@register_pipeline
def location_refinement_compositional(st1, st2, ontology,
                                      entities_refined=True):
    """Return True if there is a location-aware refinement between stmts."""
    if type(st1) != type(st2):
        return False
    if isinstance(st1, Event):
        event_ref = event_location_refinement(st1, st2, ontology,
                                              entities_refined)
        return event_ref
    elif isinstance(st1, Influence):
        subj_ref = event_location_refinement(st1.subj, st2.subj,
                                             ontology, entities_refined,
                                             ignore_polarity=True)
        obj_ref = event_location_refinement(st1.obj, st2.obj,
                                            ontology, entities_refined,
                                            ignore_polarity=True)
        delta_refinement = st1.delta_refinement_of(st2)
        return delta_refinement and subj_ref and obj_ref
    else:
        compositional_refinement(st1, st2, ontology, entities_refined)


@register_pipeline
def make_compositional_refinement_filter(ontology, nproc=None):
    return CompositionalRefinementFilter(ontology, nproc=nproc)


@register_pipeline
def make_default_compositional_refinement_filter():
    return CompositionalRefinementFilter(comp_ontology, nproc=None)


class CompositionalRefinementFilter(RefinementFilter):
    # FIXME: if we have events here, we need to be able to handle them
    def __init__(self, ontology, nproc=None):
        super().__init__()
        self.ontology = ontology
        self.nproc = nproc

    def initialize(self, stmts_by_hash):
        super().initialize(stmts_by_hash)
        # Mapping agent keys to statement hashes
        agent_key_to_hash = {}
        # Mapping statement hashes to agent keys
        hash_to_agent_key = {}
        # All agent keys for a given agent role
        all_keys_by_role = {}
        comp_idxes = list(range(4))
        # Take one statement to get the relevant roles
        # FIXME: here we assume that all statements are of the same type
        # which may not be the case if we have standalone events.
        if stmts_by_hash:
            roles = stmts_by_hash[next(iter(stmts_by_hash))]._agent_order
        # In the corner case that there are no initial statements, we just
        # assume we are working with Influences
        else:
            roles = Influence._agent_order
        # Initialize agent key data structures
        for role in roles:
            agent_key_to_hash[role] = {}
            hash_to_agent_key[role] = {}
            for comp_idx in comp_idxes:
                agent_key_to_hash[role][comp_idx] = \
                    collections.defaultdict(set)
                hash_to_agent_key[role][comp_idx] = \
                    collections.defaultdict(set)
        # Extend agent key data structures
        self._extend_maps(roles, stmts_by_hash, agent_key_to_hash,
                          hash_to_agent_key, all_keys_by_role)
        self.shared_data['agent_key_to_hash'] = agent_key_to_hash
        self.shared_data['hash_to_agent_key'] = hash_to_agent_key
        self.shared_data['all_keys_by_role'] = all_keys_by_role
        self.shared_data['roles'] = roles

    @staticmethod
    def _extend_maps(roles, stmts_by_hash, agent_key_to_hash,
                     hash_to_agent_key, all_keys_by_role):
        comp_idxes = list(range(4))
        # Fill up agent key data structures
        for sh, stmt in stmts_by_hash.items():
            for role in roles:
                agents = getattr(stmt, role)
                for comp_idx in comp_idxes:
                    agent_keys = {get_agent_key(agent, comp_idx) for agent in
                                  (agents if isinstance(agents, list)
                                   else [agents])}
                    for agent_key in agent_keys:
                        agent_key_to_hash[role][comp_idx][agent_key].add(sh)
                        hash_to_agent_key[role][comp_idx][sh].add(agent_key)
        for role in roles:
            all_keys_by_role[role] = {}
            for comp_idx in comp_idxes:
                all_keys_by_role[role][comp_idx] = \
                    set(agent_key_to_hash[role][comp_idx].keys())

    def extend(self, stmts_by_hash):
        if not stmts_by_hash:
            return
        roles = stmts_by_hash[next(iter(stmts_by_hash))]._agent_order
        self._extend_maps(roles, stmts_by_hash,
                          self.shared_data['agent_key_to_hash'],
                          self.shared_data['hash_to_agent_key'],
                          self.shared_data['all_keys_by_role'])
        # We can assume that these stmts_by_hash are unique
        self.shared_data['stmts_by_hash'].update(stmts_by_hash)

    def get_related(self, stmt, possibly_related=None,
                    direction='less_specific'):
        sh = stmt.get_hash()
        all_keys_by_role = self.shared_data['all_keys_by_role']
        agent_key_to_hash = self.shared_data['agent_key_to_hash']
        hash_to_agent_key = self.shared_data['hash_to_agent_key']
        relevants = \
            get_relevants_for_stmt(sh,
                                   all_keys_by_role=all_keys_by_role,
                                   agent_key_to_hash=agent_key_to_hash,
                                   hash_to_agent_key=hash_to_agent_key,
                                   ontology=self.ontology,
                                   direction=direction)
        assert all(isinstance(r, int) for r in relevants)

        return relevants


def get_relevants_for_stmt(sh, all_keys_by_role, agent_key_to_hash,
                           hash_to_agent_key, ontology, direction):
    relevants = None
    # We now iterate over all the agent roles in the given statement
    # type
    for role, comp_idx_hashes in hash_to_agent_key.items():
        for comp_idx, hash_to_agent_key_for_role in \
                hash_to_agent_key[role].items():
            # We get all the agent keys in all other statements that the
            # agent in this role in this statement can be a refinement of.
            for agent_key in hash_to_agent_key_for_role[sh]:
                relevant_keys = get_relevant_keys(
                    agent_key,
                    all_keys_by_role[role][comp_idx],
                    ontology=ontology,
                    direction=direction)
                # In the first iteration, we initialize the set with the
                # relevant statement hashes
                if relevants is None:
                    # Here we have to take a full union of potentially refined
                    # hashes (removing the statement itself).
                    relevants = set.union(
                        *[agent_key_to_hash[role][comp_idx][rel]
                          for rel in relevant_keys]) - {sh}
                # If not none but an empty set then we can stop
                # here
                elif not relevants:
                    break
                # In subsequent iterations, we take the intersection of
                # the relevant sets per role
                else:
                    # We start with an empty set and add to it any potentially
                    # refined hashes
                    role_relevant_stmt_hashes = set()
                    for rel in relevant_keys:
                        # We take the intersection of potentially refined
                        # statements with ones we already know are relevant,
                        # and add it to the union
                        role_relevant_stmt_hashes |= \
                            (agent_key_to_hash[role][comp_idx][rel] & relevants)
                    # Since we already took all the intersections with relevants
                    # in the loop, we can just set relevants to
                    # role_relevant_stmt_hashes and remove the statement
                    # itself
                    relevants = role_relevant_stmt_hashes - {sh}
    return relevants


@register_pipeline
def event_compositional_refinement(st1, st2, ontology, entities_refined,
                                   ignore_polarity=False):
    gr1 = concept_matches_compositional(st1.concept)
    gr2 = concept_matches_compositional(st2.concept)
    refinement = True
    # If they are both just string names, we require equality
    if not isinstance(gr1, tuple) and not isinstance(gr2, tuple):
        return gr1 == gr2
    # Otherwise we compare the tuples
    for entry1, entry2 in zip(gr1, gr2):
        # If the potentially refined value is None, it is
        # always potentially refined.
        if entry2 is None:
            continue
        # A None can never be the refinement of a not-None
        # value so we can break out here with no refinement
        elif entry1 is None:
            refinement = False
            break
        # Otherwise the values can still be equal which we allow
        # for refinement purposes
        elif entry1 == entry2:
            continue
        # Finally, the only way there is a refinement is if entry1
        # isa entry2
        else:
            if not ontology.isa('WM', entry1, 'WM', entry2):
                refinement = False
                break
    if not refinement:
        return False

    if ignore_polarity:
        return True
    pol_ref = (st1.delta.polarity and not st2.delta.polarity) or \
              st1.delta.polarity == st2.delta.polarity
    return pol_ref


@register_pipeline
def event_location_refinement(st1, st2, ontology, entities_refined,
                              ignore_polarity=False):
    """Return True if there is a location-aware refinement between Events."""
    ref = st1.refinement_of(st2, ontology, entities_refined,
                            ignore_polarity=ignore_polarity)
    if not ref:
        return False
    if not has_location(st2):
        return True
    elif not has_location(st1):
        return False
    else:
        loc1 = get_location(st1)
        loc2 = get_location(st2)
        if loc1 == loc2:
            return True
        elif isinstance(loc1, list):
            if set(loc2).issubset(set(loc1)):
                return True
    return False


@register_pipeline
def location_refinement(st1, st2, ontology, entities_refined):
    """Return True if there is a location-aware refinement between stmts."""
    if type(st1) != type(st2):
        return False
    if isinstance(st1, Event):
        event_ref = event_location_refinement(st1, st2, ontology,
                                              entities_refined)
        return event_ref
    elif isinstance(st1, Influence):
        subj_ref = event_location_refinement(st1.subj, st2.subj,
                                             ontology, entities_refined,
                                             ignore_polarity=True)
        obj_ref = event_location_refinement(st1.obj, st2.obj,
                                            ontology, entities_refined,
                                            ignore_polarity=True)
        delta_refinement = st1.delta_refinement_of(st2)
        return delta_refinement and subj_ref and obj_ref
    else:
        return st1.refinement_of(st2, ontology, entities_refined)


@register_pipeline
def event_location_time_refinement(st1, st2, ontology, entities_refined):
    """Return True if there is a location/time refinement between Events."""
    ref = location_refinement(st1, st2, ontology, entities_refined)
    if not ref:
        return False
    if not has_time(st2):
        return True
    elif not has_time(st1):
        return False
    else:
        return st1.context.time.refinement_of(st2.context.time)


@register_pipeline
def location_time_refinement(st1, st2, ontology, entities_refined):
    """Return True if there is a location/time refinement between stmts."""
    if type(st1) != type(st2):
        return False
    if isinstance(st1, Event):
        return event_location_time_refinement(st1, st2, ontology,
                                              entities_refined)
    elif isinstance(st1, Influence):
        ref = st1.refinement_of(st2, ontology)
        if not ref:
            return False
        subj_ref = event_location_time_refinement(st1.subj, st2.subj,
                                                  ontology, entities_refined)
        obj_ref = event_location_time_refinement(st1.obj, st2.obj,
                                                 ontology, entities_refined)
        return subj_ref and obj_ref


@register_pipeline
def event_location_time_delta_refinement(st1, st2, ontology, entities_refined):
    loc_time_ref = event_location_time_refinement(st1, st2, ontology,
                                                  entities_refined)
    if not loc_time_ref:
        return False
    if not st2.delta:
        return True
    elif not st1.delta:
        return False
    else:
        return st1.delta.refinement_of(st2.delta)


@register_pipeline
def location_time_delta_refinement(st1, st2, ontology, entities_refined):
    if isinstance(st1, Event):
        return event_location_time_delta_refinement(st1, st2, ontology,
                                                    entities_refined)
    elif isinstance(st1, Influence):
        ref = st1.refinement_of(st2, ontology)
        if not ref:
            return False
        subj_ref = event_location_time_delta_refinement(st1.subj, st2.subj,
                                                        ontology,
                                                        entities_refined)
        obj_ref = event_location_time_delta_refinement(st1.obj, st2.obj,
                                                       ontology,
                                                       entities_refined)
        return subj_ref and obj_ref
    else:
        return st1.refinement_of(st2, ontology, entities_refined)


def get_agent_key(agent, comp_idx):
    """Return a key for an Agent for use in refinement finding.

    Parameters
    ----------
    agent : indra.statements.Agent or None
         An INDRA Agent whose key should be returned.

    Returns
    -------
    tuple or None
        The key that maps the given agent to the ontology, with special
        handling for ungrounded and None Agents.
    """
    # Possibilities are as follows:
    # Case 1: no grounding, in which case we use the agent name as a theme
    # grounding. If we are looking at another component, we return None.
    # Case 2: There is a WM compositional grounding in which case we return
    # the specific entry in the compositional tuple if available, or None if
    # not.
    if isinstance(agent, Event):
        agent = agent.concept
    gr = agent.get_grounding(ns_order=['WM'])
    if gr[0] is None:
        if comp_idx == 0:
            agent_key = ('NAME', agent.name)
        else:
            agent_key = None
    else:
        comp_gr = gr[1][comp_idx]
        agent_key = ('WM', comp_gr) if comp_gr else None
    return agent_key
