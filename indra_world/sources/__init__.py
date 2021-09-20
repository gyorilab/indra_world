import tqdm
import pickle
import logging
import functools
from multiprocessing import Pool
from indra_world.sources import eidos, hume, sofia

logger = logging.getLogger(__name__)


def _reader_wrapper(fname, reader, dart_ids=None, **kwargs):
    if reader == 'eidos':
        pr = eidos.process_json_file(fname, **kwargs)
        pr.doc.tree = None
    elif reader == 'sofia':
        pr = sofia.process_json_file(fname, **kwargs)
    elif reader == 'hume':
        pr = hume.process_jsonld_file(fname, **kwargs)
    if dart_ids:
        dart_id = dart_ids.get(fname)
        for stmt in pr.statements:
            for ev in stmt.evidence:
                ev.text_refs['DART'] = dart_id
    return pr.statements


def process_reader_outputs(fnames, reader,
                           dart_ids=None,
                           extract_filter=None,
                           grounding_mode='compositional',
                           nproc=8,
                           output_pkl=None):
    if extract_filter is None:
        extract_filter = ['influence']

    pool = Pool(nproc)
    chunk_size = 10
    process_fun = functools.partial(_reader_wrapper,
                                    reader=reader, dart_ids=dart_ids,
                                    extract_filter=extract_filter,
                                    grounding_mode=grounding_mode)

    stmts = []
    for res in tqdm.tqdm(pool.imap_unordered(process_fun, fnames,
                                             chunksize=chunk_size),
                         total=len(fnames)):
        stmts += res

    logger.debug('Closing pool...')
    pool.close()
    logger.debug('Joining pool...')
    pool.join()
    logger.info('Pool closed and joined.')

    if output_pkl:
        logger.info(f'Writing into {output_pkl}')
        with open(output_pkl, 'wb') as fh:
            pickle.dump(stmts, fh)
    return stmts