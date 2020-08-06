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
    curator.submit_curation(curations[0])

    # test factor_grounding
    assembled_stmts = curator.run_assembly(corp_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']

    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']

    # Test vet statement
    # curator.submit_curation(curations[1])
    # assembled_stmts = curator.run_assembly(corp_id, proj_id)
    # subj, obj = assembled_stmts[0].agent_list()

    # test discard statement: should remove one raw stmt
    curator.submit_curation(curations[2])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    assert len(assembled_stmts[0].evidence) == 3

    # 