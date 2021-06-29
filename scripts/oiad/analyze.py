import os
import tqdm
import glob
import pickle
from indra.statements import Event, Influence
from indra_world.sources.eidos import process_json_file

base_folder = '/Users/ben/data/wm/oiad'
folder_pattern = 'oiad_output_%s_ontology'
skip_list = ['fd0ac77c753189239c2252d88a2e30dc.jsonld']


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
    versions = ['old', 'new']
    grounding_mode = 'compositional'

    all_stmts = {}
    indexed_events = {}
    for version in versions:
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





