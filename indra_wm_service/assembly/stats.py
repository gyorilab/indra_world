import logging
from collections import defaultdict
from indra.pipeline import register_pipeline

logger = logging.getLogger(__name__)


# Some statistics functions
@register_pipeline
def print_statistics(stmts):
    ev_tot = sum([len(stmt.evidence) for stmt in stmts])
    logger.info(f'Total evidence {ev_tot} for {len(stmts)} statements.')


@register_pipeline
def print_grounding_statistics(stmts, limit=None):
    groundings = defaultdict(int)
    for stmt in stmts:
        for ag in stmt.agent_list():
            try:
                wm_highest = ag.db_refs['WM'][0][0]
                groundings[wm_highest] += 1
            except KeyError:
                continue
    logger.info('Grounding concepts and their counts')
    for grounding, count in sorted(groundings.items(), key=lambda x: x[1],
                                   reverse=True)[:limit]:
        logger.info(f'{grounding} : {count}')


@register_pipeline
def print_document_statistics(stmts):
    doc_ids = set()
    for stmt in stmts:
        doc_id = stmt.evidence[0].annotations['provenance'][0]['document']['@id']
        assert len(doc_id) == 32
        doc_ids.add(doc_id)
    logger.info(
        f'Extracted {len(stmts)} statements from {len(doc_ids)} documents')

