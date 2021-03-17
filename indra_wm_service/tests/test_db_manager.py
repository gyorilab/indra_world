from .test_incremental_assember import s1, s2, s1h, s2h
from indra_wm_service.db.manager import DbManager


def _get_db():
    db = DbManager('sqlite:///:memory:')
    db.create_all()
    return db


def test_add_project_documents():
    db = _get_db()
    db.add_project(0, 'My Project')
    db.add_documents_for_project(0, ['abc', 'def'])
    docs = db.get_documents_for_project(0)
    assert docs == ['abc', 'def'], docs


def test_add_dart_record():
    db = _get_db()
    db.add_dart_record('eidos', '1.0', 'abc1', 'xyz1', 'today')
    db.add_dart_record('eidos', '1.0', 'abc2', 'xyz2', 'today')
    db.add_dart_record('hume', '2.0', 'abc1', 'xyz3', 'today')
    keys = db.get_dart_record(reader='eidos', document_id='abc1')
    assert keys == ['xyz1']
    keys = db.get_dart_record(reader='hume', document_id='abc1')
    assert keys == ['xyz3']


