import os
import tqdm
import glob
import pickle
import networkx
import matplotlib.pyplot as plt
from collections import Counter
from indra.statements import Event, Influence
from indra_world.sources.eidos import process_json_file
from indra_world.assembly.operations import compositional_grounding_filter

base_folder = '/Users/ben/data/wm/oiad'
folder_pattern = 'oiad_output_%s_ontology'
skip_list = ['fd0ac77c753189239c2252d88a2e30dc.jsonld']


def get_different_events(indexed_events):
    diff_groundings = []
    for key, old_grounding in indexed_events['old'].items():
        if key in indexed_events['new']:
            new_grounding = indexed_events['new'][key]
            if new_grounding != old_grounding:
                diff_groundings.append((old_grounding, new_grounding))
    return diff_groundings


def get_indexed_events(stmts):
    indexed_events = {}
    for stmt in stmts:
        if isinstance(stmt, Event):
            wmgr = stmt.concept.db_refs.get('WM')
            gr = None if not wmgr else wmgr[0]
            pos = stmt.evidence[0].annotations['provenance'][0]['documentCharPositions'][0]
            start = pos['start']
            end = pos['end']
            doc_id = stmt.evidence[0].text_refs['DART']
            key = (doc_id, start, end)
            indexed_events[key] = gr
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


def get_network(stmts):
    G = networkx.DiGraph()
    edges = []
    for stmt in stmts:
        if isinstance(stmt, Influence):
            edge = []
            for concept in [stmt.subj.concept, stmt.obj.concept]:
                if 'WM' not in concept.db_refs or \
                        concept.db_refs['WM'] is None:
                    key = None
                else:
                    key = tuple(entry[0] if entry else None
                                for entry in concept.db_refs['WM'][0])
                edge.append(key)
            edges.append(edge)
    G.add_edges_from(edges)
    return G


def get_num_edges_by_threshold(all_stmts):
    thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.9]
    num_edges = {}
    for version, stmts in all_stmts.items():
        num_edges[version] = []
        filtered_stmts = stmts
        for threshold in tqdm.tqdm(thresholds):
            filtered_stmts = compositional_grounding_filter(filtered_stmts,
                                                            threshold)
            nx = get_network(filtered_stmts)
            num_edges[version].append(len(nx.edges))
    return thresholds, num_edges


def plot_num_edges_by_threshold(thresholds, num_edges):
    plt.figure()
    plt.plot(thresholds, num_edges['old'], label='old')
    plt.plot(thresholds, num_edges['new'], label='new')
    plt.ylabel('Number of CAG edges')
    plt.xlabel('Grounding score threshold')
    plt.legend()
    plt.show()


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


def drop_compositional(stmts):
    for stmt in stmts:
        for agent in stmt.agent_list():
            if 'WM' in agent.db_refs:
                for grounding in agent.db_refs['WM']:
                    for pos in [1, 2, 3]:
                        grounding[pos] = None


def drop_compositional_indexed(indexed_events):
    for k, v in indexed_events.items():
        if v is None:
            continue
        for pos in [1, 2, 3]:
            v[pos] = None
        indexed_events[k] = v


if __name__ == '__main__':
    CACHED = True
    versions = ['old', 'new']
    grounding_mode = 'compositional'

    all_stmts = {}
    indexed_events = {}
    for version in versions:
        if CACHED:
            print('Loading %s pickles from cache...' % version)
            with open('%s_stmts.pkl' % version, 'rb') as fh:
                all_stmts[version] = pickle.load(fh)
            with open('%s_indexed_events.pkl' % version, 'rb') as fh:
                indexed_events[version] = pickle.load(fh)
        else:
            pattern = os.path.join(base_folder, folder_pattern % version,
                                   '*.jsonld')
            print('Finding files in %s' % pattern)
            fnames = glob.glob(pattern)
            all_stmts[version] = []
            for fname in tqdm.tqdm(fnames):
                if os.path.basename(fname) in skip_list:
                    continue
                doc_id = os.path.splitext(os.path.basename(fname))[0]
                ep = process_json_file(fname, grounding_mode=grounding_mode,
                                       extract_filter={'influence'})
                ep.extract_all_events()
                for stmt in ep.statements:
                    for ev in stmt.evidence:
                        ev.text_refs['DART'] = doc_id
                all_stmts[version] += ep.statements

            indexed_events[version] = get_indexed_events(all_stmts[version])
            with open('%s_stmts.pkl' % version, 'wb') as fh:
                pickle.dump(all_stmts[version], fh, protocol=5)
            with open('%s_indexed_events.pkl' % version, 'wb') as fh:
                pickle.dump(indexed_events[version], fh, protocol=5)

    # Number of overall events
    for version in versions:
        print('Number of overall events [%s]: %d' %
              (version, len(indexed_events[version])))
        print('Number of grounded events [%s]: %d' %
              (version, len([e for e in indexed_events[version].values()
                             if e is not None])))

    drop_compositional(all_stmts['old'])
    drop_compositional(all_stmts['new'])
    drop_compositional_indexed(indexed_events['old'])
    drop_compositional_indexed(indexed_events['new'])

    # Differential groundings
    diff_groundings = get_different_events(indexed_events)
    print('Number of groundings that differ between old and new: %d' %
          len(diff_groundings))
    # New groundings
    ng = len([d for d in diff_groundings if d[0] is None])
    print('Number of grounded concepts that were ungrounded before: %d' % ng)
    # Diff score groundings
    inc = dec = mixed = 0
    diffs = []
    for old_grounding, new_grounding in diff_groundings:
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
            diffs.append(new_grounding[0][1] - old_grounding[0][1])
    print('Number of grounding scores increased: %d' % inc)
    print('Number of grounding scores decreased: %d' % dec)
    print('Number of grounding scores mixed: %d' % mixed)

    coverage = get_term_coverage(indexed_events)
    for version in versions:
        print('Number of ontology terms grounded to [%s]: %d' %
              (version, len(coverage[version])))

    # TODO: number of unique compositional groundings

    networks = {}
    for version in versions:
        networks[version] = get_network(all_stmts[version])
        print('Number of CAG nodes [%s]: %d' %
              (version, len(networks[version])))
        print('Number of CAG edges [%s]: %d' %
              (version, len(networks[version].edges)))

    threshold = 0.6
    for version in versions:
        filtered_stmts = compositional_grounding_filter(all_stmts[version],
                                                        threshold)
        nx = get_network(filtered_stmts)
        print('Number of CAG nodes with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx)))
        print('Number of CAG edges with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx.edges)))
    thresholds, num_edges = get_num_edges_by_threshold(all_stmts)
    plot_num_edges_by_threshold(thresholds, num_edges)

