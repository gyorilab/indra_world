import logging
from typing import Any, Dict, List

from indra.pipeline import AssemblyPipeline
from indra.statements import Statement

logger = logging.getLogger(__name__)


def preprocess_statements(
    raw_statements: List[Statement],
    steps: List[Dict[str, Any]],
) -> List[Statement]:
    """Run a preprocessing pipeline on raw statements.

    Parameters
    ----------
    raw_statements :
        A list of INDRA Statements to preprocess.
    steps :
        A list of AssemblyPipeline steps that define the steps of
        preprocessing.

    Returns
    -------
    preprocessed_statements :
        A list of preprocessed INDRA Statements.
    """
    logger.info('Running preprocessing on %d statements'
                % len(raw_statements))
    ap = AssemblyPipeline(steps)
    preprocessed_statements = ap.run(raw_statements)
    logger.info('%d statements after preprocessing'
                % len(preprocessed_statements))
    return preprocessed_statements
