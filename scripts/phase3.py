import os
import sys
import glob
import tqdm
import pickle
import logging
from indra_world.corpus import Corpus
from indra_world.assembly.operations import *
from indra_world.sources.dart import process_reader_outputs
from indra.pipeline import AssemblyPipeline

logger = logging.getLogger('phase3')
HERE = os.path.dirname(os.path.abspath(__file__))


# December experiment
reader_versions = {'eidos': '1.0.5'}

DART_STORAGE = '/Users/ben/data/dart'


def load_reader_outputs(reader_versions):
    logger.info('Loading outputs based on %s' % str(reader_versions))
    reader_outputs = {}
    for reader, version in reader_versions.items():
        logger.info('Loading %s/%s' % (reader, version))
        reader_outputs[reader] = {}
        reader_folder = os.path.join(DART_STORAGE, reader, version)
        fnames = glob.glob('%s/*' % reader_folder)
        logger.info('Found %d files' % len(fnames))
        for fname in tqdm.tqdm(fnames):
            doc_id = os.path.splitext(os.path.basename(fname))[0]
            with open(fname, 'r') as fh:
                doc_str = fh.read()
                reader_outputs[reader][doc_id] = doc_str
    return reader_outputs


if __name__ == '__main__':
    corpus_id = 'phase3_eidos_v3'
    logger.info('Processing reader output...')
    reader_outputs = load_reader_outputs(reader_versions)
    stmts = process_reader_outputs(reader_outputs, corpus_id,
                                   grounding_mode='compositional',
                                   extract_filter=['influence'])
    '''
    stmts = []
    for reader in reader_versions['compositional']:
        logger.info('Loading %s' % reader)
        if os.path.exists('compositional_dec2020_%s_raw.pkl' % reader):
            with open('compositional_dec2020_%s_raw.pkl' % reader, 'rb') as fh:
                stmts += pickle.load(fh)
    '''
    logger.info('Got a total of %s statements' % len(stmts))
    assembly_config_file = os.path.join(
        HERE, os.pardir, 'indra_wm_service', 'resources',
        'assembly_compositional_phase3.json')
    pipeline = AssemblyPipeline.from_json_file(assembly_config_file)
    assembled_stmts = pipeline.run(stmts)

    num_docs = 472
    meta_data = {
        'corpus_id': corpus_id,
        'description': 'Compositional grounding assembly for the intial '
                       'Phase 3 documents, Eidos only.',
        'display_name': 'Compositional grounding assembly Phase 3 (Eidos)',
        'readers': list(reader_versions.keys()),
        'assembly': {
            'level': 'grounding_location',
            'grounding_threshold': 0.6,
        },
        'num_statements': len(assembled_stmts),
        'num_documents': num_docs
    }

    corpus = Corpus(corpus_id=corpus_id,
                    statements=assembled_stmts,
                    raw_statements=stmts,
                    meta_data=meta_data)
    corpus.s3_put()
