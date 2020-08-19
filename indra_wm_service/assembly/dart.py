import json
import logging
from indra.sources import eidos, hume, sofia, cwms


logger = logging.getLogger(__name__)


def process_reader_outputs(outputs):
    all_stmts = []
    for reader, reader_outputs in outputs.items():
        logger.info('Processing %d outputs for %s' %
                    (len(reader_outputs), reader))
        for doc_id, reader_output_str in reader_outputs.items():
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
        if pr is not None:
            all_stmts += pr.statements
    return all_stmts
