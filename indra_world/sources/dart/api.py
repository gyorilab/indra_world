__all__ = ['process_reader_output', 'process_reader_outputs',
           'print_record_stats', 'get_record_key', 'get_unique_records']

import os
import json
import pickle
import logging
from collections import Counter
from indra.statements import Statement
from indra_world.sources import eidos, hume, sofia, cwms


logger = logging.getLogger(__name__)


def fix_provenance(stmts, doc_id):
    """Move the document identifiers in evidences."""
    for stmt in stmts:
        for ev in stmt.evidence:
            ev.text_refs['DART'] = doc_id
            # Sometime the PMID is set which is not appropriate so
            # we reset it
            ev.pmid = None
    return stmts


def process_reader_output(reader, reader_output_str, doc_id,
                          grounding_mode, extract_filter):
    if reader == 'eidos':
        pr = eidos.process_json_str(reader_output_str,
                                    grounding_mode=grounding_mode,
                                    extract_filter=extract_filter)
    elif reader == 'hume':
        jld = json.loads(reader_output_str)
        pr = hume.process_jsonld(jld, grounding_mode=grounding_mode,
                                 extract_filter=extract_filter)
    elif reader == 'sofia':
        jd = json.loads(reader_output_str)
        pr = sofia.process_json(jd, grounding_mode=grounding_mode,
                                extract_filter=extract_filter)
    elif reader == 'cwms':
        pr = cwms.process_ekb(reader_output_str, grounding_mode=grounding_mode,
                              extract_filter=extract_filter)
    else:
        raise ValueError('Unknown reader %s' % reader)
    if pr is not None:
        stmts = fix_provenance(pr.statements, doc_id)
        return stmts
    else:
        return []


def process_reader_outputs(outputs, corpus_id=None,
                           grounding_mode='compositional',
                           extract_filter=None):
    if not extract_filter:
        extract_filter = ['influence']
    all_stmts = []
    for reader, reader_outputs in outputs.items():
        if corpus_id:
            fname = '%s_%s_raw.pkl' % (corpus_id, reader)
            if os.path.exists(fname):
                with open(fname, 'rb') as fh:
                    all_stmts += pickle.load(fh)
                    continue
        logger.info('Processing %d outputs for %s' %
                    (len(reader_outputs), reader))
        reader_stmts = []
        for doc_id, reader_output_str in reader_outputs.items():
            reader_stmts += process_reader_output(reader,
                                                  reader_output_str,
                                                  doc_id,
                                                  grounding_mode=grounding_mode,
                                                  extract_filter=extract_filter)
        if corpus_id:
            with open(fname, 'wb') as fh:
                pickle.dump(reader_stmts, fh)
        all_stmts += reader_stmts
    assert all(isinstance(stmt, Statement) for stmt in all_stmts)
    return all_stmts


def print_record_stats(recs):
    """Print statistics for a list of DART records."""
    print("reader,tenants,reader_version,ontology_version,count")
    for (reader, tenants, reader_version, ontology_version), count in sorted(
            Counter([get_record_key(rec) for rec in recs]).items(),
            key=lambda x: x[1], reverse=True):
        print(
            f"{reader},{'|'.join(tenants)},{reader_version},"
            f"{ontology_version},{count}"
        )


def get_record_key(rec):
    """Return a key for a DART record for purposes of deduplication."""
    return (rec['identity'], tuple(sorted(rec['tenants'])), rec['version'],
            rec['output_version'])


def get_unique_records(recs):
    """Deduplicate DART records based on an identifier key."""
    return list({(get_record_key(rec), rec['document_id']): rec
                 for rec in recs}.values())


