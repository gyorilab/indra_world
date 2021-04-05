import os
import glob
import uuid
import random
from indra_world.service.app import sc
from indra_world.service.db.manager import DbManager
from indra_world.service.corpus_manager import CorpusManager
from .test_rest_api import _call_api

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_DB_FILE = os.path.join(HERE, 'workflow_test.db')
TEST_DB_URL = f'sqlite:///{TEST_DB_FILE}'


def test_end_to_end():
    try:
        os.remove(TEST_DB_FILE)
    except FileNotFoundError:
        pass
    sc.db = DbManager(url=TEST_DB_URL)
    sc.db.create_all()

    # Populate the records here
    dart_base = os.path.join(os.path.expanduser('~'), 'data', 'dart')
    print(f'Dart base: {dart_base}')
    readers = ['eidos', 'sofia', 'hume']
    versions = ['1.1.0', '1.2_compositional',
                'r2021_03_15.7ddc68e6.compositional.r1']
    all_records = []
    for reader, version in zip(readers, versions):
        fnames = glob.glob(os.path.join(dart_base, reader, version, '*'))
        doc_ids = [os.path.basename(fname) for fname in fnames]
        # These are totally synthetic, they have to be unique but are otherwise
        # arbitrary when using local storage for DART
        storage_keys = [str(uuid.uuid4()) for _ in doc_ids]
        records = [{'identity': reader,
                    'version': version,
                    'storage_key': storage_key,
                    'document_id': doc_id}
                   for storage_key, doc_id in zip(storage_keys, doc_ids)]
        all_records += records
    # Choose a random sample for an initial corpus
    random.seed(123)
    random.shuffle(all_records)
    LIMIT = 10
    corpus_records = all_records[:LIMIT]
    cm = CorpusManager(db_url=TEST_DB_URL,
                       dart_records=corpus_records,
                       corpus_id='test_corpus',
                       metadata={},
                       local_storage=dart_base)
    cm.prepare()
    os.environ['INDRA_WM_CACHE'] = dart_base
    res = _call_api('post', 'assembly/new_project',
                    json={'project_id': 'test_project',
                          'project_name': 'my project',
                          'corpus_id': 'test_corpus'})
    res = _call_api('post', 'dart/notify',
                    json=all_records[LIMIT])
    assert res == 'OK'
    res = _call_api('post', 'assembly/add_project_records',
                    json={'project_id': 'test_project',
                          'records': [all_records[LIMIT]]})
    assert set(res) == {'new_stmts', 'new_evidence', 'new_refinements',
                        'beliefs'}
    assert len(res['new_stmts']) == 7, len(res['new_stmts'])
    assert len(res['new_evidence']) == 7
    assert len(res['new_refinements']) == 2
    assert len(res['beliefs']) == 307
