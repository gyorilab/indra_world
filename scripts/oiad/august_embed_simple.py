"""This script is a simplified analysis of the August embed, specifically
looking at changes related to the NAF use case which were made
between version 2.1 and 2.2. Only Eidos is used and only the theme
is retained instead of the full compositional grounding."""
import os
import glob
import tqdm
import pickle
import networkx
from collections import Counter
from indra.statements import Event, Influence
from indra_world.sources import eidos
from indra_world.assembly.operations import compositional_grounding_filter


def plot_hist_diffs(diffs):
    import matplotlib.pyplot as plt
    plt.figure()
    plt.hist(diffs, 200, color='blue', alpha=0.8)
    plt.xlabel('Grounding score change after taxonomy update', fontsize=14)
    plt.ylabel('Number of concept groundings', fontsize=14)
    plt.axvline(x=0, color='red', linestyle='--')
    plt.xlim([-0.8, 0.8])
    plt.show()


def fix_provenance(fname, stmts):
    doc_id = os.path.basename(fname).split('.')[0]
    for stmt in stmts:
        for ev in stmt.evidence:
            ev.text_refs['DART'] = doc_id


def get_indexed_event(event):
    wmgr = event.concept.db_refs.get('WM')
    gr = None if not wmgr else wmgr[0]
    doc_char_pos = event.evidence[0].annotations['provenance'][0]['documentCharPositions']
    if isinstance(doc_char_pos, list):
        pos = doc_char_pos[0]
    else:
        pos = doc_char_pos
    doc_id = event.evidence[0].text_refs['DART']
    key = (doc_id, pos['start'], pos['end'])
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
            if (entry1[0] != entry2[0] and
                entry1[0].split('/')[-1] != entry2[0].split('/')[-1]) or \
                    (abs(entry1[1]-entry2[1]) > 1e-3):
                return False
    return True


def grounding_terms_match(grt1, grt2):
    if grt1 == grt2 or (grt1 is not None and grt2 is not None and
            grt1.split('/')[-1] == grt2.split('/')[-1]):
        return True
    return False


def get_different_events(indexed_events):
    diff_groundings = []
    for key, old_grounding in indexed_events['2.1'].items():
        if key in indexed_events['2.2']:
            new_grounding = indexed_events['2.2'][key]
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


def drop_compositional(stmts):
    for stmt in stmts:
        for agent in stmt.agent_list():
            if 'WM' in agent.db_refs:
                for grounding in agent.db_refs['WM']:
                    for pos in [1, 2, 3]:
                        grounding[pos] = None


def score_change_for_unchanged_grounding(diff_groundings):
    unchanged_grounding_score_diffs = []
    for old_grounding, new_grounding, _ in diff_groundings:
        if old_grounding is not None and new_grounding is not None and \
                grounding_terms_match(old_grounding[0][0], new_grounding[0][0]):
            unchanged_grounding_score_diffs.append(
                new_grounding[0][1] - old_grounding[0][1]
            )
    return unchanged_grounding_score_diffs


def get_all_terms_grounded_to(indexed_events):
    return set(grounding[0][0][0] for grounding in indexed_events.values()
               if grounding is not None and grounding[0] is not None)


def count_grounded_to_new_terms(diff_groundings, all_terms_grounded_to):
    shared_endpoints = \
        {t.split('/')[-1] for t in all_terms_grounded_to['2.2']} & \
            {t.split('/')[-1] for t in all_terms_grounded_to['2.1']}
    diff_grounded_to = {t for t in (all_terms_grounded_to['2.2'] -
                                    all_terms_grounded_to['2.1'])
                        if t.split('/')[-1] not in shared_endpoints}
    count = 0
    for old_grounding, new_grounding, _ in diff_groundings:
        if new_grounding is not None and \
                new_grounding[0][0] in diff_grounded_to:
            count += 1
    return count


DART_PATH = '/home/ben/data/dart/eidos'
#DART_PATH = '/Users/ben/Downloads/jan2022redo'
#DART_PATH = '/Users/ben/tmp/oiad'

if __name__ == '__main__':
    versions = ['2.1', '2.2']
    indexed_events = {}
    all_terms_grounded_to = {}
    all_stmts = {'2.1': [], '2.2': []}
    for version in versions:
        version_pkl = os.path.join(DART_PATH, '%s_stmts.pkl' % version)
        if os.path.exists(version_pkl):
            with open(version_pkl, 'rb') as fh:
                all_stmts[version] = pickle.load(fh)
        else:
            files = glob.glob(os.path.join(DART_PATH, version, '*.jsonld'))
            print('Processing %s' % version)
            for fname in tqdm.tqdm(files):
                # First we just extract influences
                ep = eidos.process_json_file(fname, grounding_mode='compositional',
                                             extract_filter={'influence'})
                # Then separately we extract all events including standalone
                # and subsumed ones
                ep.extract_all_events()
                # Fix DART document IDs
                fix_provenance(fname, ep.statements)
                all_stmts[version] += ep.statements
            drop_compositional(all_stmts[version])
            with open(version_pkl, 'wb') as fh:
                pickle.dump(all_stmts[version], fh)
        indexed_events[version] = get_indexed_events(all_stmts[version])
        all_terms_grounded_to[version] = \
            get_all_terms_grounded_to(indexed_events[version])

    for version in versions:
        print('All terms grounded to [%s]: %d' %
              (version, len(all_terms_grounded_to[version])))

    diff_groundings = get_different_events(indexed_events)

    unchanged_grounding_score_diffs = \
        score_change_for_unchanged_grounding(diff_groundings)

    grounded_to_new_term_count = \
        count_grounded_to_new_terms(diff_groundings, all_terms_grounded_to)

    # Diff score groundings
    inc = dec = unchanged = 0
    diffs = []
    for old_grounding, new_grounding, text in diff_groundings:
        if old_grounding is None and new_grounding is not None:
            inc += 1
            diffs.append(new_grounding[0][1])
        elif old_grounding is not None and new_grounding is None:
            dec += 1
            diffs.append(-old_grounding[0][1])
        else:
            # Note we exploit here that we just take the theme
            old_theme, old_score = old_grounding[0]
            new_theme, new_score = new_grounding[0]
            # Some of the entries were just moved around but aren't real
            # extensions, we can ignore these as spurious
            if old_theme.split('/')[-1] == new_theme.split('/')[-1]:
                continue
            if old_score < new_score:
                inc += 1
            elif new_score < old_score:
                dec += 1
            else:
                unchanged += 1
            diffs.append(new_score - old_score)
    print('Number of grounding scores increased: %d (%2.f)' %
          (inc, inc/len(diffs)))
    print('Number of grounding scores decreased: %d' % dec)
    print('Number of grounding scores unchanged: %d' % unchanged)

    threshold = 0.6
    for version in versions:
        print('Running filter at threshold %.2f for %s' % (threshold, version))
        filtered_stmts = compositional_grounding_filter(all_stmts[version],
                                                        threshold)
        nx = get_network(filtered_stmts)
        print('Number of CAG nodes with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx)))
        print('Number of CAG edges with grounding threshold %.2f [%s]: %d' %
              (threshold, version, len(nx.edges)))

    plot_hist_diffs()


