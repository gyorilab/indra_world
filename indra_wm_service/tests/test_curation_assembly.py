import os
import json
from indra_wm_service.corpus import Corpus
from indra_wm_service.curator import LiveCurator
from indra.statements import *
from indra.tools import assemble_corpus as ac
from indra.ontology.world.ontology import world_ontology

HERE = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(HERE, 'test_curations.json'), 'r') as fh:
    curations = json.load(fh)


def _make_curator(curation_idx):
    cur = curations[curation_idx]
    subj = Concept(cur['before']['subj']['factor'],
                   db_refs={'WM': cur['before']['subj']['concept'],
                            'TEXT': cur['before']['subj']['factor']})
    obj = Concept(cur['before']['obj']['factor'],
                  db_refs={'WM': cur['before']['obj']['concept'],
                           'TEXT': cur['before']['obj']['factor']})
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

    assembled_stmts = ac.run_preassembly([stmt])

    corpus = Corpus('dart-20200313-interventions-grounding',
                    statements=assembled_stmts, raw_statements=[stmt])

    world_ontology.initialize()
    curator = LiveCurator(
        corpora={'dart-20200313-interventions-grounding': corpus},
        ont_manager=world_ontology,
        eidos_url='http://eidos.cs.arizona.edu:9000'
    )

    return curator


proj_id = 'project-0c970384-9f57-4ded-a535-96b613811a89'
corp_id = 'dart-20200313-interventions-grounding'


def test_factor_grounding():
    curator = _make_curator(0)
    # test factor_grounding
    curator.submit_curation(curations[0])
    assembled_stmts = curator.run_assembly(corp_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']

    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[0]['after']['subj']['concept']


def test_vet_statement():
    curator = _make_curator(1)
    # Test vet statement: curation 1
    curator.submit_curation(curations[1])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    stmt = assembled_stmts[0]
    assert stmt.belief == 1


def test_discard_statement():
    curator = _make_curator(2)
    # test discard statement: curation 2
    curator.submit_curation(curations[2])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    assert len(assembled_stmts) == 0


def test_reverse_relation():
    curator = _make_curator(4)
    # Test reverse relation: curation 4
    curator.submit_curation(curations[4])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    subj, obj = assembled_stmts[0].agent_list()
    assert subj.get_grounding()[1] == curations[4]['before']['obj']['concept']
    assert obj.get_grounding()[1] == curations[4]['before']['subj']['concept']


def test_factor_polarity():
    curator = _make_curator(6)
    # Factor polarity: curation 6
    curator.submit_curation(curations[6])
    assembled_stmts = curator.run_assembly(corp_id, proj_id)
    stmt = assembled_stmts[0]
    assert stmt.overall_polarity() == \
        curations[6]['after']['subj']['polarity'] * \
        curations[6]['after']['obj']['polarity']
