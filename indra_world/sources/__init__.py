import tqdm
import pickle
import logging
import functools
from typing import List, Mapping, Optional
from multiprocessing import Pool
from indra.statements import Statement
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


def process_reader_outputs(fnames: List[str],
                           reader: str,
                           dart_ids: Mapping[str, str] = None,
                           extract_filter: List[str] = None,
                           grounding_mode: str = 'compositional',
                           nproc: int = 8,
                           output_pkl: str = None) -> List[Statement]:
    """Process a set of reader outputs in parallel.

    Parameters
    ----------
    fnames :
        The list of file paths to the reader outputs to be processed.
    reader :
        The name of the reader which produced the outputs.
    dart_ids :
        A dict which maps each fname in the fnames list to a DART document ID.
        These are then set in the evidences of statements exxtracted from
        the output.
    extract_filter :
        What types of statements to extract.
    grounding_mode :
        The type of grounding mode to use for processing.
    nproc :
        The number of workers to use for parallelization.
    output_pkl :
        The path to an output pickle file in which to dump the statements
        extracted from the outputs.

    Returns
    -------
    :
        The list of statements extracted from the outputs.
    """
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