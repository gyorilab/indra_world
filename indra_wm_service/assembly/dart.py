import os
import json
import tqdm
import pickle
import logging
from indra.statements import Statement
from indra.sources import eidos, hume, sofia, cwms


logger = logging.getLogger(__name__)


def fix_provenance(stmts, doc_id):
    """Move the document identifiers in evidences."""
    for stmt in stmts:
        for ev in stmt.evidence:
            if 'provenance' not in ev.annotations:
                ev.annotations['provenance'] = [{'document': {'@id': doc_id}}]
            else:
                prov = ev.annotations['provenance'][0]['document']
                prov['@id'] = doc_id
    return stmts


def process_reader_outputs(outputs, corpus_id):
    all_stmts = []
    for reader, reader_outputs in outputs.items():
        fname = '%s_%s_raw.pkl' % (corpus_id, reader)
        if os.path.exists(fname):
            with open(fname, 'rb') as fh:
                all_stmts += pickle.load(fh)
                continue

        reader_stmts = []
        logger.info('Processing %d outputs for %s' %
                    (len(reader_outputs), reader))
        for doc_id, reader_output_str in tqdm.tqdm(reader_outputs.items()):
            if reader == 'eidos':
                pr = eidos.process_json_str(reader_output_str)
            elif reader == 'hume':
                jld = json.loads(reader_output_str)
                pr = hume.process_jsonld(jld)
            elif reader == 'sofia':
                # FIXME: is this the right way to process Sofia output?
                jd = json.loads(reader_output_str)
                pr = sofia.process_json(jd)
            elif reader == 'cwms':
                pr = cwms.process_ekb(reader_output_str)
            else:
                continue
            if pr is not None:
                reader_stmts += fix_provenance(pr.statements, doc_id)
        with open(fname, 'wb') as fh:
            pickle.dump(reader_stmts, fh)
        all_stmts += reader_stmts
    assert all(isinstance(stmt, Statement) for stmt in all_stmts)
    return all_stmts
