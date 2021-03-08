import os
import yaml
import copy
import logging
import itertools
import statistics
import collections
from datetime import datetime
from collections import defaultdict
import indra.tools.assemble_corpus as ac
from indra.sources.eidos.client import reground_texts
from indra.pipeline import register_pipeline
from indra.statements import Influence, Association, Event
from indra.preassembler import get_agent_key, get_relevant_keys
from indra.statements.concept import get_sorted_compositional_groundings
from indra.preassembler.custom_preassembly import event_location_refinement, \
    get_location
from indra.preassembler.refinement import RefinementFilter
from indra.ontology.world.ontology import WorldOntology

logger = logging.getLogger(__name__)


register_pipeline(datetime)

comp_onto_branch = '4531c084d3b902f04605c11396a25db4fff16573'
comp_ontology_url = 'https://raw.githubusercontent.com/WorldModelers/'\
                    'Ontologies/%s/CompositionalOntology_v2.1_metadata.yml' % \
    comp_onto_branch
comp_ontology = WorldOntology(comp_ontology_url)


@register_pipeline
def get_expanded_events_influences(stmts):
    """Return a list of all standalone events from a list of statements."""
    events_influences = []
    for stmt_orig in stmts:
        stmt = copy.deepcopy(stmt_orig)
        if isinstance(stmt, Influence):
            for member in [stmt.subj, stmt.obj]:
                member.evidence = stmt.evidence[:]
                # Remove the context since it may be for the other member
                for ev in member.evidence:
                    ev.context = None
                events_influences.append(member)
            # We add the Influence too
            events_influences.append(stmt_orig)
        elif isinstance(stmt, Association):
            for member in stmt.members:
                member.evidence = stmt.evidence[:]
                # Remove the context since it may be for the other member
                for ev in member.evidence:
                    ev.context = None
                events_influences.append(member)
        elif isinstance(stmt, Event):
            events_influences.append(stmt)
    return events_influences


@register_pipeline
def remove_namespaces(stmts, namespaces):
    """Remove unnecessary namespaces from Concept grounding."""
    logger.info('Removing unnecessary namespaces')
    for stmt in stmts:
        for agent in stmt.agent_list():
            for namespace in namespaces:
                if namespace in copy.deepcopy(agent.db_refs):
                    agent.db_refs.pop(namespace, None)
    logger.info('Finished removing unnecessary namespaces')
    return stmts


@register_pipeline
def remove_raw_grounding(stmts):
    """Remove the raw_grounding annotation to decrease output size."""
    for stmt in stmts:
        for ev in stmt.evidence:
            if not ev.annotations:
                continue
            agents = ev.annotations.get('agents')
            if not agents:
                continue
            if 'raw_grounding' in agents:
                agents.pop('raw_grounding', None)
    return stmts


@register_pipeline
def check_event_context(events):
    for event in events:
        if not event.context and event.evidence[0].context:
            assert False, ('Event context issue', event, event.evidence)
        ej = event.to_json()
        if 'context' not in ej and 'context' in ej['evidence'][0]:
            assert False, ('Event context issue', event, event.evidence)


@register_pipeline
def reground_stmts(stmts, ont_manager, namespace, eidos_service=None,
                   overwrite=True, sources=None):
    ont_manager.initialize()
    if sources is None:
        sources = {'sofia', 'cwms'}
    if eidos_service is None:
        eidos_service = 'http://localhost:9000'
    logger.info(f'Regrounding {len(stmts)} statements')
    # Send the latest ontology and list of concept texts to Eidos
    yaml_str = yaml.dump(ont_manager.yml)
    concepts = []
    for stmt in stmts:
        # Skip statements from sources that shouldn't be regrounded
        if not any(ev.source_api in sources for ev in stmt.evidence):
            continue
        for concept in stmt.agent_list():
            concept_txt = concept.db_refs.get('TEXT')
            concepts.append(concept_txt)
    logger.info(f'Finding grounding for {len(concepts)} texts')
    groundings = reground_texts(concepts, yaml_str,
                                webservice=eidos_service)
    # Update the corpus with new groundings
    idx = 0
    logger.info(f'Setting new grounding for {len(stmts)} statements')
    for stmt in stmts:
        # Skip statements from sources that shouldn't be regrounded
        if not any(ev.source_api in sources for ev in stmt.evidence):
            continue
        for concept in stmt.agent_list():
            if overwrite:
                if groundings[idx]:
                    concept.db_refs[namespace] = groundings[idx]
                elif namespace in concept.db_refs:
                    concept.db_refs.pop(namespace, None)
            else:
                if (namespace not in concept.db_refs) and groundings[idx]:
                    concept.db_refs[namespace] = groundings[idx]
            idx += 1
    logger.info(f'Finished setting new grounding for {len(stmts)} statements')
    return stmts


@register_pipeline
def remove_hume_redundant(stmts, matches_fun):
    logger.info(f'Removing Hume redundancies on {len(stmts)} statements.')
    raw_stmt_groups = defaultdict(list)
    for stmt in stmts:
        sh = stmt.get_hash(matches_fun=matches_fun, refresh=True)
        eh = (stmt.evidence[0].pmid, stmt.evidence[0].text,
              stmt.subj.concept.name, stmt.obj.concept.name,
              stmt.evidence[0].annotations['adjectives'])
        key = str((sh, eh))
        raw_stmt_groups[key].append(stmt)
    new_stmts = list({group[0] for group in raw_stmt_groups.values()})
    logger.info(f'{len(new_stmts)} statements after filter.')
    return new_stmts


@register_pipeline
def fix_wm_ontology(stmts):
    for stmt in stmts:
        for concept in stmt.agent_list():
            if 'WM' in concept.db_refs:
                concept.db_refs['WM'] = [(entry.replace(' ', '_'), score)
                                         for entry, score in
                                         concept.db_refs['WM']]


@register_pipeline
def filter_context_date(stmts, from_date=None, to_date=None):
    logger.info(f'Filtering dates on {len(stmts)} statements')
    if not from_date and not to_date:
        return stmts
    new_stmts = []
    for stmt in stmts:
        doc_id = \
            stmt.evidence[0].annotations['provenance'][0]['document']['@id']
        if isinstance(stmt, Influence):
            events = [stmt.subj, stmt.obj]
        elif isinstance(stmt, Association):
            events = stmt.members
        else:
            events = [stmt]
        for event in events:
            if event.context and event.context.time:
                if from_date and event.context.time.start and \
                        (event.context.time.start < from_date):
                    logger.info(f'Removing date {event.context.time.start}'
                                f'({event.context.time.text}) from {doc_id}')
                    event.context.time = None
                if to_date and event.context.time.end and \
                        (event.context.time.end > to_date):
                    event.context.time = None
                    logger.info(f'Removing date {event.context.time.end}'
                                f'({event.context.time.text}) from {doc_id}')
        new_stmts.append(stmt)
    logger.info(f'{len(new_stmts)} statements after date filter')
    return new_stmts


@register_pipeline
def filter_groundings(stmts):
    excl_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             os.pardir, 'resources',
                             'groundings_to_exclude.txt')
    with open(excl_file, 'r') as f:
        groundings_to_exclude = [l.strip() for l in f.readlines()]
    stmts = ac.filter_by_db_refs(
        stmts, 'WM', groundings_to_exclude, 'all', invert=True)
    return stmts


def deduplicate_groundings(groundings):
    groundings = get_sorted_compositional_groundings(groundings)
    # TODO: sometimes we have duplication which is not exact, rather,
    # the same grounding (after filtering) is present but with a different
    # score. We could eliminate these by retaining only the one with the
    # highest ranking.
    return list(gr for gr, _ in itertools.groupby(groundings))


def compositional_grounding_filter_stmt(stmt, score_threshold,
                                        groundings_to_exclude):
    stmt = copy.deepcopy(stmt)
    for concept in stmt.agent_list():
        if concept is not None and 'WM' in concept.db_refs:
            wm_groundings = copy.copy(concept.db_refs['WM'])
            new_groundings = []
            for idx, gr in enumerate(wm_groundings):
                for jdx, entry in enumerate(gr):
                    if entry is not None:
                        if (entry[0] in groundings_to_exclude or
                                entry[1] < score_threshold):
                            if isinstance(wm_groundings[idx], tuple):
                                wm_groundings[idx] = \
                                    list(wm_groundings[idx])
                            wm_groundings[idx][jdx] = None
                # Promote dangling property
                if wm_groundings[idx][0] is None and \
                        wm_groundings[idx][1] is not None:
                    wm_groundings[idx][0] = wm_groundings[idx][1]
                    wm_groundings[idx][1] = None
                # Promote process
                if wm_groundings[idx][0] is None and \
                        wm_groundings[idx][2] is not None:
                    wm_groundings[idx][0] = wm_groundings[idx][2]
                    wm_groundings[idx][2] = None
                    if wm_groundings[idx][3] is not None:
                        wm_groundings[idx][1] = wm_groundings[idx][3]
                        wm_groundings[idx][3] = None
                # Remove dangling process property
                if wm_groundings[idx][3] is not None and \
                        wm_groundings[idx][2] is None:
                    wm_groundings[idx][3] = None
                if not all(entry is None for entry in wm_groundings[idx]):
                    new_groundings.append(wm_groundings[idx])
            new_groundings = deduplicate_groundings(new_groundings)
            concept.db_refs['WM'] = new_groundings
            # Get rid of all None tuples
            concept.db_refs['WM'] = [
                gr for gr in concept.db_refs['WM']
                if not all(g is None for g in gr)
            ]
            # Pop out the WM key if there is no grounding at all
            if not concept.db_refs['WM']:
                return None
        else:
            return None
    validate_grounding_format([stmt])
    return stmt


@register_pipeline
def compositional_grounding_filter(stmts, score_threshold,
                                   groundings_to_exclude=None):
    groundings_to_exclude = groundings_to_exclude \
        if groundings_to_exclude else []
    stmts_out = []
    for stmt in stmts:
        stmt_out = compositional_grounding_filter_stmt(stmt, score_threshold,
                                                       groundings_to_exclude)
        if stmt_out:
            stmts_out.append(stmt_out)
    return stmts_out


@register_pipeline
def standardize_names_compositional(stmts):
    for stmt in stmts:
        for concept in stmt.agent_list():
            comp_grounding = concept.db_refs['WM'][0]
            disp_name = make_display_name(comp_grounding)
            concept.name = disp_name
    return stmts


@register_pipeline
def add_flattened_grounding_compositional(stmts):
    for stmt in stmts:
        for concept in stmt.agent_list():
            wm_flat = []
            for comp_grounding in concept.db_refs['WM']:
                theme_grounding = comp_grounding[0][0]
                other_groundings = [entry[0].split('/')[-1]
                                    for entry in comp_grounding[1:] if entry]
                flat_grounding = '_'.join([theme_grounding] + other_groundings)
                standard_name = make_display_name(comp_grounding)
                score = statistics.mean([entry[1] for entry in comp_grounding
                                         if entry is not None])
                wm_flat.append(
                    {
                        'grounding': flat_grounding,
                        'name': standard_name,
                        'score': score
                    }
                )
            concept.db_refs['WM_FLAT'] = wm_flat
    return stmts


@register_pipeline
def validate_grounding_format(stmts):
    for idx, stmt in enumerate(stmts):
        for concept in stmt.agent_list():
            if 'WM' not in concept.db_refs:
                continue
            wms = concept.db_refs['WM']
            assert isinstance(wms, list)
            wm = wms[0]
            assert len(wm) == 4
            assert wm[0] is not None
            if wm[2] is None:
                assert wm[3] is None, (idx, stmt, stmt.evidence[0].source_api,
                                       stmt.evidence[0].annotations, wm)
    return stmts


def make_display_name(comp_grounding):
    entries = tuple(entry[0].split('/')[-1].replace('_', ' ')
                    if entry else None for entry in comp_grounding)
    entries_reversed = [entry for entry in entries[::-1] if
                        entry is not None]
    return ' of '.join(entries_reversed)


@register_pipeline
def set_positive_polarities(stmts):
    for stmt in stmts:
        if isinstance(stmt, Influence):
            for event in [stmt.subj, stmt.obj]:
                if event.delta.polarity is None:
                    event.delta.polarity = 1
    return stmts


@register_pipeline
def filter_out_long_words(stmts, k=10):
    logger.info(f'Filtering to concepts with max {k} words on {len(stmts)}'
                f' statements.')

    def get_text(ag):
        return ag.db_refs['TEXT']

    def text_too_long(txt, k):
        if len(txt.split()) > k:
            return True
        return False

    new_stmts = []
    for stmt in stmts:
        if any(text_too_long(get_text(c), k) for c in stmt.agent_list()):
            continue
        new_stmts.append(stmt)
    logger.info(f'{len(new_stmts)} statements after filter.')
    return new_stmts


@register_pipeline
def concept_matches_compositional(concept):
    wm = concept.db_refs.get('WM')
    if not wm:
        return concept.name
    wm_top = tuple(entry[0] if entry else None for entry in wm[0])
    return wm_top


@register_pipeline
def matches_compositional(stmt):
    if isinstance(stmt, Influence):
        key = (stmt.__class__.__name__,
               concept_matches_compositional(stmt.subj.concept),
               concept_matches_compositional(stmt.obj.concept),
               stmt.polarity_count(),
               stmt.overall_polarity()
               )
    elif isinstance(stmt, Event):
        key = (stmt.__class__.__name__,
               concept_matches_compositional(stmt.concept),
               stmt.delta.polarity)
    # TODO: handle Associations?
    return str(key)


@register_pipeline
def location_matches_compositional(stmt):
    """Return a matches_key which takes geo-location into account."""
    if isinstance(stmt, Event):
        context_key = get_location(stmt)
        matches_key = str((matches_compositional(stmt), context_key))
    elif isinstance(stmt, Influence):
        subj_context_key = get_location(stmt.subj)
        obj_context_key = get_location(stmt.obj)
        matches_key = str((matches_compositional(stmt), subj_context_key,
                           obj_context_key))
    else:
        matches_key = matches_compositional(stmt)
    return matches_key


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
def make_default_compositional_refinement_filer():
    return CompositionalRefinementFilter(comp_ontology, nproc=None)


class CompositionalRefinementFilter(RefinementFilter):
    def __init__(self, ontology, nproc=None):
        super().__init__()
        self.ontology = ontology
        self.nproc = nproc

    def initialize(self, stmts_by_hash):
        super().initialize(stmts_by_hash)
        # Take one statement to get the relevant roles
        roles = stmts_by_hash[next(iter(stmts_by_hash))]._agent_order
        # Mapping agent keys to statement hashes
        agent_key_to_hash = {}
        # Mapping statement hashes to agent keys
        hash_to_agent_key = {}
        # All agent keys for a given agent role
        all_keys_by_role = {}
        comp_idxes = list(range(4))
        for role in roles:
            agent_key_to_hash[role] = {}
            hash_to_agent_key[role] = {}
            for comp_idx in comp_idxes:
                agent_key_to_hash[role][comp_idx] = \
                    collections.defaultdict(set)
                hash_to_agent_key[role][comp_idx] = \
                    collections.defaultdict(set)

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
        self.shared_data['agent_key_to_hash'] = agent_key_to_hash
        self.shared_data['hash_to_agent_key'] = hash_to_agent_key
        self.shared_data['all_keys_by_role'] = all_keys_by_role
        self.shared_data['roles'] = roles

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
                # We now get the actual statement hashes that these other
                # potentially refined agent keys appear in in the given role
                role_relevant_stmt_hashes = set.union(
                    *[agent_key_to_hash[role][comp_idx][rel]
                      for rel in relevant_keys]) - {sh}
                # In the first iteration, we initialize the set with the
                # relevant statement hashes
                if relevants is None:
                    relevants = role_relevant_stmt_hashes
                # If not none but an empty set then we can stop
                # here
                elif not relevants:
                    break
                # In subsequent iterations, we take the intersection of
                # the relevant sets per role
                else:
                    relevants &= role_relevant_stmt_hashes
    return relevants


@register_pipeline
def sort_compositional_groundings(statements):
    for stmt in statements:
        for concept in stmt.agent_list():
            if 'WM' in concept.db_refs:
                concept.db_refs['WM'] = \
                    get_sorted_compositional_groundings(concept.db_refs['WM'])
    return statements


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


@register_pipeline
def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    else:
        return obj