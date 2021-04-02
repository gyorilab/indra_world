import logging
from indra.pipeline import AssemblyPipeline


logger = logging.getLogger(__name__)


def preprocess_statements(raw_statements, steps):
    logger.info('Running preprocessing on %d statements'
                % len(raw_statements))
    ap = AssemblyPipeline(steps)
    preprocessed_statements = ap.run(raw_statements)
    logger.info('%d statements after preprocessing'
                % len(preprocessed_statements))
    return preprocessed_statements
