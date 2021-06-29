import os
import tqdm
import glob
import pickle
from indra.statements import Event, Influence
from indra_world.sources.eidos import process_json_file

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


if __name__ == '__main__':
    CACHED = True
    versions = ['old', 'new']
    grounding_mode = 'compositional'

    all_stmts = {}
    indexed_events = {}
    for version in versions:
        if CACHED:
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
    # Differential groundings
    diff_groundings = get_different_events(indexed_events)
    # New groundings
    ng = len([d for d in diff_groundings if d[0] is None])
    print('Number of new groundings: %d' % ng)
    # Diff score groundings
    inc = dec = mixed = 0
    for old_grounding, new_grounding in diff_groundings:
        if old_grounding is None and new_grounding is not None:
            inc += 1
        elif old_grounding is not None and new_grounding is None:
            dec += 1
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
    print('Number of grounding scores increased: %d' % inc)
    print('Number of grounding scores decreased: %d' % dec)
    print('Number of grounding scores mixed: %d' % mixed)


