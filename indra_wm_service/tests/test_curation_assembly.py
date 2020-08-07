import json
from indra_wm_service.corpus import Corpus
from indra_wm_service.curator import LiveCurator
from indra.statements import *
from indra.tools import assemble_corpus as ac
from indra.ontology.world.ontology import world_ontology

with open('test_curations.json', 'r') as fh:
    curations = json.load(fh)


def _make_curator():
    stmts = []
    for cur in curations:
        subj = Concept(cur['before']['subj']['factor'],
                       db_refs={'WM': cur['before']['subj']['concept']})
        obj = Concept(cur['before']['obj']['factor'],
                      db_refs={'WM': cur['before']['obj']['concept']})
        subj_delta = QualitativeDelta(polarity=cur['before']['subj']['polarity'])
        subj_event = Event(subj, delta=subj_delta)
        obj_delta = QualitativeDelta(polarity=cur['before']['obj']['polarity'])
        obj_event = Event(obj, delta=obj_delta)

        evidence = [
            Evidence(source_api=reader)
            for reader in cur['before']['wm']['readers']
        ]
        stmt = Influence(subj_event, obj_event, evidence=evidence)
        stmt.uuid = cur['statement_id']
        stmts.append(stmt)

    assembled_stmts = ac.run_preassembly(stmts)

    corpus = Corpus('dart-20200313-interventions-grounding',
                    statements=assembled_stmts, raw_statements=stmts)

    curator = LiveCurator(
        corpora={'dart-20200313-interventions-grounding': corpus},
        ont_manager=world_ontology
    )
    return curator


def test_curation_assembly():
    proj_id = 'project-0c970384-9f57-4ded-a535-96b613811a89'
    corp_id = 'dart-20200313-interventions-grounding'
    curator = _make_curator()
    corpus = curator.corpora[corp_id]
    init_evidence_count = len(corpus.statements[0].evidence)
    curator.submit_curation(curations[0])

    # test factor_grounding
    assembled_stmts = curator.run_assembly(corp_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']

    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']

    # Test vet statement: curation 1
    # curator.submit_curation(curations[1])
    # assembled_stmts = curator.run_assembly(corp_id, proj_id)
    # stmt = assembled_stmts[0]
    # fixme what do we expect from a "correct" curation? belief == 1?

    # test discard statement: curation 2
    curator.submit_curation(curations[2])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    assert len(assembled_stmts[0].evidence) == init_evidence_count-1

    # Test reverse relation: curation 4
    curator.submit_curation(curations[4])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[4]['before']['obj']['concept']
    assert obj.get_grounding()[1] == curations[4]['before']['subj']['concept']

    # Factor polarity: curation 6
    curator.submit_curation(curations[5])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    stmt = assembled_stmts[0]
    assert stmt.overall_polarity() == \
        curations[5]['after']['subj']['polarity'] * \
        curations[5]['after']['obj']['polarity']
