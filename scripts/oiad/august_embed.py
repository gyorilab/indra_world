import os
import glob
import tqdm
import pickle
import networkx
from collections import Counter
from indra_world.sources.dart import DartClient
from indra.statements import Event, Influence
from indra.statements import stmts_from_json_file
from indra_world.sources import eidos, hume, sofia
from indra_world.assembly.operations import compositional_grounding_filter


data_path = '/home/ben/data/wm/august_embed_2021'
corpus_paths = {
    '10k': os.path.join(data_path, 'august_embed_10k.json'),
    'new_america': os.path.join(data_path, 'august_embed_new-america.json'),
    'ata': os.path.join(data_path, 'august_embed_ata.json')
}

readers = ['eidos', 'hume', 'sofia']


def filter_to_doc_ids(stmts, doc_ids):
    return [stmt for stmt in stmts
            if any([ev.text_refs['DART'] in doc_ids for ev in stmt.evidence])]


def get_indexed_event(event):
    wmgr = event.concept.db_refs.get('WM')
    gr = None if not wmgr else wmgr[0]
    if 'provenance' not in event.evidence[0].annotations:
        return None, None, None
    doc_char_pos = event.evidence[0].annotations['provenance'][0]['documentCharPositions']
    if isinstance(doc_char_pos, list):
        pos = doc_char_pos[0]
    else:
        pos = doc_char_pos
    start = pos['start']
    end = pos['end']
    if 'DART' in event.evidence[0].text_refs:
        doc_id = event.evidence[0].text_refs['DART']
    elif 'title' in event.evidence[0].annotations['provenance'][0]['document']:
        doc_id = event.evidence[0].annotations['provenance'][0]['document']['title']
    else:
        return None, None, None
    reader = event.evidence[0].source_api
    key = (reader, doc_id, start, end)
    return key, gr, event.concept.db_refs['TEXT']


def get_indexed_events(stmts):
    indexed_events = {}
    for stmt in stmts:
        if isinstance(stmt, Event):
            key, gr, text = get_indexed_event(stmt)
            if not key:
                continue
            indexed_events[key] = (gr, text)
    return indexed_events


def get_term_coverage(indexed_events):
    coverage = {}
    for version, ind_events in indexed_events.items():
        terms = []
        for grounding in ind_events.values():
            if grounding is not None:
                for entry in grounding:
                    if entry is not None:
                        terms.append(entry[0])
        coverage[version] = Counter(terms)
    return coverage


def get_grounding_scores(indexed_events):
    scores = {}
    for version, idx_events in indexed_events.items():
        scores[version] = []
        for grounding in idx_events.values():
            if grounding is not None:
                for entry in grounding:
                    if entry is not None:
                        scores[version].append(entry[1])
    return scores


def groundings_match(gr1, gr2):
    if gr1[0] is None and gr2[0] is not None:
        return False
    elif gr2[0] is None and gr1[0] is not None:
        return False
    elif gr1[0] is None and gr2[0] is None:
        return True
    for entry1, entry2 in zip(gr1[0], gr2[0]):
        if entry1 is None or entry2 is None:
            if entry1 != entry2:
                return False
        else:
            if (entry1[0] != entry2[0]) or (abs(entry1[1]-entry1[1]) > 1e-3):
                return False
    return True


def get_different_events(indexed_events):
    diff_groundings = []
    for reader in ['eidos', 'hume']:
        for key, old_grounding in indexed_events['%s_2.3' % reader].items():
            if key in indexed_events['%s_3.0' % reader]:
                new_grounding = indexed_events['%s_3.0' % reader][key]
                if not groundings_match(old_grounding, new_grounding):
                    diff_groundings.append((old_grounding[0], new_grounding[0],
                                            old_grounding[1]))
    return diff_groundings


def get_network(stmts):
    G = networkx.DiGraph()
    edges = []
    for stmt in tqdm.tqdm(stmts):
        if isinstance(stmt, Influence):
            edge = []
            for concept in [stmt.subj.concept, stmt.obj.concept]:
                if 'WM' not in concept.db_refs or \
                        concept.db_refs['WM'] is None:
                    key = 'None'
                else:
                    key = tuple(entry[0] if entry else None
                                for entry in concept.db_refs['WM'][0])
                edge.append(key)
            edges.append(edge)
    G.add_edges_from(edges)
    return G



#if __name__ == '__main__':
#    dc = DartClient()
#    recs = dc.get_reader_output_records(readers=['eidos'])
#    recs_10k = [rec for rec in recs if rec['output_version'] == '2.3']
#    doc_ids_10k = {rec['document_id'] for rec in recs_10k}
#    corpus_stmts = {k: stmts_from_json_file(v, format='jsonl')
#                    for k, v in corpus_paths.items()}
#    new_america_filtered = filter_to_doc_ids(corpus_stmts['new_america'],
#                                             doc_ids_10k)
#    ata_filtered = filter_to_doc_ids(corpus_stmts['ata'], doc_ids_10k)


if __name__ == '__main__':
    readers = ['eidos', 'hume', 'sofia']
    versions = ['2.3', '3.0']
    indexed_events = {}
    all_stmts = {'2.3': [], '3.0': []}
    for reader in readers:
        for version in versions:
            print('Processing %s %s' % (reader, version))
            with open(os.path.join(data_path,
                                   '%s_%s.pkl' % (reader, version)), 'rb') as fh:
                stmts = pickle.load(fh)
                all_stmts[version] += stmts
            indexed_events['%s_%s' % (reader, version)] = get_indexed_events(stmts)

    diff_groundings = get_different_events(indexed_events)

    # Diff score groundings
    inc = dec = mixed = 0
    diffs = []
    for old_grounding, new_grounding, text in diff_groundings:
        if old_grounding is None and new_grounding is not None:
            inc += 1
            diffs.append(new_grounding[0][1])
        elif old_grounding is not None and new_grounding is None:
            dec += 1
            diffs.append(-old_grounding[0][1])
        else:
            if all([old_entry is None or (old_entry[1] <= new_entry[1])
                    for old_entry, new_entry in
                    zip(old_grounding, new_grounding)
                    if new_entry is not None]):
                inc += 1
            elif all([new_entry is None or (new_entry[1] <= old_entry[1])
                     for old_entry, new_entry in
                     zip(old_grounding, new_grounding)
                     if old_entry is not None]):
                dec += 1
            else:
                mixed = 1
            diff = sum([
                (new[1] if new is not None else 0) -
                (old[1] if old is not None else 0)
            for old, new in zip(old_grounding, new_grounding)])
            diffs.append(diff)
    print('Number of grounding scores increased: %d' % inc)
    print('Number of grounding scores decreased: %d' % dec)
    print('Number of grounding scores mixed: %d' % mixed)

    threshold = 0.6
    for version in versions:
        filtered_stmts = compositional_grounding_filter(all_stmts[version],
                                                        threshold)
        nx = get_network(filtered_stmts)
        print('Number of CAG nodes with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx)))
        print('Number of CAG edges with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx.edges)))