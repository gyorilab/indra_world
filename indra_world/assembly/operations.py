__all__ = ['get_expanded_events_influences', 'remove_namespaces',
           'remove_raw_grounding', 'check_event_context', 'reground_stmts',
           'remove_hume_redundant', 'fix_wm_ontology', 'filter_context_date',
           'filter_groundings', 'deduplicate_groundings',
           'compositional_grounding_filter_stmt',
           'compositional_grounding_filter', 'standardize_names_compositional',
           'add_flattened_grounding_compositional', 'validate_grounding_format',
           'make_display_name', 'make_display_name_linear',
           'set_positive_polarities',
           'filter_out_long_words', 'concept_matches_compositional',
           'matches_compositional', 'location_matches_compositional',
           'event_compositional_refinement', 'compositional_refinement',
           'location_refinement_compositional',
           'make_compositional_refinement_filter',
           'make_default_compositional_refinement_filter',
           'CompositionalRefinementFilter', 'get_relevants_for_stmt',
           'listify', 'merge_deltas']
import os
import yaml
import copy
import logging
import itertools
import statistics
from datetime import datetime
from collections import defaultdict
import indra.tools.assemble_corpus as ac
from indra.pipeline import register_pipeline
from indra.statements import Influence, Association, Event
from indra.statements.concept import get_sorted_compositional_groundings
from indra_world.sources.eidos.client import reground_texts

from .matches import *
from .refinement import *

logger = logging.getLogger(__name__)


register_pipeline(datetime)


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
        doc_id = stmt.evidence[0].text_refs.get('DART')
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
                                        groundings_to_exclude,
                                        remove_self_loops=False):
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
                # If we have a theme and want to remove self loops
                # i.e., where both the theme and the process/property
                # are the same, we remove the process/property
                if remove_self_loops and wm_groundings[idx][0]:
                    # Theme and property are the same: remove property
                    if wm_groundings[idx][1] and \
                            (wm_groundings[idx][0][0] ==
                             wm_groundings[idx][1][0]):
                        wm_groundings[idx][1] = None
                    # Theme and process are the same: remove process
                    if wm_groundings[idx][2] and \
                            (wm_groundings[idx][0][0] ==
                             wm_groundings[idx][2][0]):
                        wm_groundings[idx][2] = None
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
                                   groundings_to_exclude=None,
                                   remove_self_loops=False):
    groundings_to_exclude = groundings_to_exclude \
        if groundings_to_exclude else []
    stmts_out = []
    for stmt in stmts:
        stmt_out = compositional_grounding_filter_stmt(stmt, score_threshold,
                                                       groundings_to_exclude,
                                                       remove_self_loops=remove_self_loops)
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
    """Return display name from a compositional grounding with 'of' linkers."""
    entries = tuple(entry[0].split('/')[-1].replace('_', ' ')
                    if entry else None for entry in comp_grounding)
    entries_reversed = [entry for entry in entries[::-1] if
                        entry is not None]
    return ' of '.join(entries_reversed)


def make_display_name_linear(comp_grounding):
    """Return display name from compositional grounding with linear joining."""
    entries = tuple(entry[0].split('/')[-1].replace('_', ' ')
                    if entry else None for entry in comp_grounding)
    entries = [entry for entry in entries if entry is not None]
    return ' '.join(entries)


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
def sort_compositional_groundings(statements):
    for stmt in statements:
        for concept in stmt.agent_list():
            if 'WM' in concept.db_refs:
                concept.db_refs['WM'] = \
                    get_sorted_compositional_groundings(concept.db_refs['WM'])
    return statements


@register_pipeline
def merge_deltas(stmts_in):
    """Gather and merge original Influence delta information from evidence.


    This function is only applicable to Influence Statements that have
    subj and obj deltas. All other statement types are passed through unchanged.
    Polarities and adjectives for subjects and objects respectivey are
    collected and merged by travesrsing all evidences of a Statement.

    Parameters
    ----------
    stmts_in : list[indra.statements.Statement]
        A list of INDRA Statements whose influence deltas should be merged.
        These Statements are meant to have been preassembled and potentially
        have multiple pieces of evidence.

    Returns
    -------
    stmts_out : list[indra.statements.Statement]
        The list of Statements now with deltas merged at the Statement
        level.
    """
    stmts_out = []
    for stmt in stmts_in:
        # This operation is only applicable to Influences
        if not isinstance(stmt, Influence):
            stmts_out.append(stmt)
            continue
        # At this point this is guaranteed to be an Influence
        deltas = {}
        for role in ('subj', 'obj'):
            for info in ('polarity', 'adjectives'):
                key = (role, info)
                deltas[key] = []
                for ev in stmt.evidence:
                    entry = ev.annotations.get('%s_%s' % key)
                    deltas[key].append(entry if entry else None)
        # POLARITY
        # For polarity we need to work in pairs
        polarity_pairs = list(zip(deltas[('subj', 'polarity')],
                                  deltas[('obj', 'polarity')]))
        # If we have some fully defined pairs, we take the most common one
        both_pols = [pair for pair in polarity_pairs if pair[0] is not None and
                     pair[1] is not None]
        if both_pols:
            subj_pol, obj_pol = max(set(both_pols), key=both_pols.count)
            stmt.subj.delta.polarity = subj_pol
            stmt.obj.delta.polarity = obj_pol
        # Otherwise we prefer the case when at least one entry of the
        # pair is given
        else:
            one_pol = [pair for pair in polarity_pairs if pair[0] is not None or
                       pair[1] is not None]
            if one_pol:
                subj_pol, obj_pol = max(set(one_pol), key=one_pol.count)
                stmt.subj.delta.polarity = subj_pol
                stmt.obj.delta.polarity = obj_pol

        # ADJECTIVES
        for attr, role in ((stmt.subj.delta, 'subj'), (stmt.obj.delta, 'obj')):
            all_adjectives = []
            for adj in deltas[(role, 'adjectives')]:
                if isinstance(adj, list):
                    all_adjectives += adj
                elif adj is not None:
                    all_adjectives.append(adj)
            attr.adjectives = all_adjectives
        stmts_out.append(stmt)
    return stmts_out


@register_pipeline
def listify(obj):
    if not isinstance(obj, list):
        return [obj]
    else:
        return obj
