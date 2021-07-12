from .test_incremental_assembler import s1, s2
from indra_world.service.db.manager import DbManager


def _get_db():
    db = DbManager('sqlite:///:memory:')
    db.create_all()
    return db


def test_project_records():
    db = _get_db()
    db.add_project('p1', 'My Project')
    db.add_records_for_project('p1', ['abc', 'def'])
    recs = db.get_records_for_project('p1')
    assert recs == ['abc', 'def'], recs


def test_dart_record():
    db = _get_db()
    db.add_dart_record('eidos', '1.0', 'abc1', 'xyz1', 'today')
    db.add_dart_record('eidos', '1.0', 'abc2', 'xyz2', 'today')
    db.add_dart_record('hume', '2.0', 'abc1', 'xyz3', 'today')
    keys = db.get_dart_records(reader='eidos', document_id='abc1')
    assert keys == ['xyz1']
    keys = db.get_dart_records(reader='hume', document_id='abc1')
    assert keys == ['xyz3']


def test_dart_records_extended():
    db = _get_db()
    db.add_dart_record('eidos', '1.0', 'd1', 'stk1', '2021',
                       '1.2', 'embed|test|xyz', 'tenant1')
    db.add_dart_record('eidos', '1.0', 'd1', 'stk2', '2021',
                       '1.3', 'embed|test|abc', 'tenant2|tenant3')

    keys = db.get_dart_records(reader='eidos', document_id='d1')
    assert set(keys) == {'stk1', 'stk2'}

    keys = db.get_dart_records(reader='eidos', document_id='d1',
                               output_version='1.3')
    assert set(keys) == {'stk2'}

    keys = db.get_dart_records(reader='eidos', document_id='d1',
                               labels={'abc'})
    assert set(keys) == {'stk2'}

    keys = db.get_dart_records(reader='eidos', document_id='d1',
                               labels={'abc', 'xyz'})
    assert set(keys) == set()

    keys = db.get_dart_records(reader='eidos', document_id='d1',
                               labels={'embed'})
    assert set(keys) == {'stk1', 'stk2'}

    keys = db.get_dart_records(reader='eidos', document_id='d1',
                               tenants=['tenant3'])
    assert set(keys) == {'stk2'}


def test_statements():
    db = _get_db()
    db.add_dart_record('eidos', '1.0', 'abc1', 'xyz1', 'today')
    db.add_statements_for_record(record_key='xyz1',
                                 indra_version='1.0',
                                 stmts=[s1, s2])
    stmts = db.get_statements_for_record('xyz1')
    assert len(stmts) == 2
    # Hashes are different in the DB due to no matches_fun used in
    # serialization
    assert {s.uuid for s in stmts} == {s1.uuid, s2.uuid}


def test_curations():
    db = _get_db()
    db.add_curation_for_project('p1', 12345, {'x': 'y'})
    curs = db.get_curations_for_project('p1')
    assert len(curs) == 1
    assert curs[12345] == {'x': 'y'}


def test_project_records_unique():
    db = _get_db()
    db.add_project('p1', 'project 1')
    db.add_dart_record('hume', '2.0', 'abc1', 'xyz3', 'today')
    db.add_records_for_project('p1', ['xyz3'])
    db.add_records_for_project('p1', ['xyz3'])
    records = db.get_records_for_project('p1')
    assert len(records) == 1


def test_corpus_records_unique():
    db = _get_db()
    db.add_corpus('c1', 'corpus 1')
    db.add_dart_record('hume', '2.0', 'abc1', 'xyz3', 'today')
    db.add_records_for_corpus('c1', ['xyz3'])
    db.add_records_for_corpus('c1', ['xyz3'])
    records = db.get_records_for_corpus('c1')
    assert len(records) == 1


def test_add_no_stmts_for_record():
    db = _get_db()
    db.add_statements_for_record(record_key='x', stmts=[s1, s2],
                                 indra_version='1.0')
    stmts = db.get_statements()
    assert len(stmts) == 2
    db.add_statements_for_record(record_key='x', stmts=[],
                                 indra_version='1.0')
    stmts = db.get_statements()
    assert len(stmts) == 2


def test_get_projects():
    db = _get_db()
    projects = db.get_projects()
    assert not projects
    db.add_project('p1', 'Project 1')
    db.add_project('p2', 'Project 2')
    projects = db.get_projects()
    assert len(projects) == 2
    assert {p['id'] for p in projects} == {'p1', 'p2'}
    assert {p['name'] for p in projects} == {'Project 1', 'Project 2'}
