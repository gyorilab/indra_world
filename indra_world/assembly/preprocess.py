import logging
from indra.pipeline import AssemblyPipeline


logger = logging.getLogger(__name__)


def preprocess_statements(raw_statements, steps):
    """Run a preprocessing pipeline on raw statements.

    Parameters
    ----------
    raw_statements : list[indra.statements.Statement]
        A list of INDRA Statements to preprocess.
    steps : list[dict]
        A list of AssemblyPipeline steps that define the steps of
        preprocessing.

    Returns
    -------
    list[indra.statements.Statement]
        A list of preprocessed INDRA Statements.
    """
    logger.info('Running preprocessing on %d statements'
                % len(raw_statements))
    ap = AssemblyPipeline(steps)
    preprocessed_statements = ap.run(raw_statements)
    logger.info('%d statements after preprocessing'
                % len(preprocessed_statements))
    return preprocessed_statements
