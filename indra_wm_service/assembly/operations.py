import os
import yaml
import copy
import logging
import statistics
from datetime import datetime
from collections import defaultdict
import indra.tools.assemble_corpus as ac
from indra.sources.eidos.client import reground_texts
from indra.pipeline import register_pipeline
from indra.statements import Influence, Association, Event

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


def compositional_grounding_filter_stmt(stmt, score_threshold,
                                        groundings_to_exclude):
    for concept in stmt.agent_list():
        if concept is not None and 'WM' in concept.db_refs:
            wm_groundings = concept.db_refs['WM']
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
            concept.db_refs['WM'] = wm_groundings
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
    for stmt in stmts:
        for concept in stmt.agent_list():
            if 'WM' not in concept.db_refs:
                continue
            wms = concept.db_refs['WM']
            assert isinstance(wms, list)
            wm = wms[0]
            assert len(wm) == 4
            assert wm[0] is not None
            if wm[2] is None:
                assert wm[3] is None
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
