from .test_incremental_assember import s1, s2, s1h, s2h
from indra_wm_service.db.manager import DbManager


def _get_db():
    db = DbManager('sqlite:///:memory:')
    db.create_all()
    return db


def test_project_documents():
    db = _get_db()
    db.add_project(0, 'My Project')
    db.add_documents_for_project(0, ['abc', 'def'])
    docs = db.get_documents_for_project(0)
    assert docs == ['abc', 'def'], docs


def test_dart_record():
    db = _get_db()
    db.add_dart_record('eidos', '1.0', 'abc1', 'xyz1', 'today')
    db.add_dart_record('eidos', '1.0', 'abc2', 'xyz2', 'today')
    db.add_dart_record('hume', '2.0', 'abc1', 'xyz3', 'today')
    keys = db.get_dart_record(reader='eidos', document_id='abc1')
    assert keys == ['xyz1']
    keys = db.get_dart_record(reader='hume', document_id='abc1')
    assert keys == ['xyz3']


def test_statements():
    db = _get_db()
    db.add_statements_for_document(document_id='d1',
                                   reader='eidos',
                                   reader_version='1.0',
                                   indra_version='1.0',
                                   stmts=[s1, s2])
    stmts = db.get_statements_for_document(document_id='d1', reader='hume')
    assert not stmts
    stmts = db.get_statements_for_document(document_id='d1', reader='eidos')
    assert len(stmts) == 2
    # Hashes are different in the DB due to no matches_fun used in
    # serialization
    assert {s.uuid for s in stmts} == {s1.uuid, s2.uuid}


def test_curations():
    db = _get_db()
    db.add_curation_for_project(0, {'curation': {'x': 'y'}})
    curs = db.get_curations_for_project(0)
    assert len(curs) == 1
    assert curs[0]['curation'] == {'x': 'y'}
