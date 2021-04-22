import json
from nose.tools import raises
from datetime import datetime
from unittest.mock import patch
from indra_world.service.app import api
from indra_world.service.app import sc
from indra_world.service.db.manager import DbManager
from .test_service_controller import _get_eidos_output


def _call_api(method, route, *args, **kwargs):
    tc = api.app.test_client()
    req_meth = getattr(tc, method)
    start = datetime.now()
    print("Submitting request to '%s' at %s." % ('/' + route, start))
    print("\targs:", args)
    print("\tkwargs:", kwargs)
    res = req_meth(route, *args, **kwargs)
    end = datetime.now()
    print("Got result with %s at %s after %s seconds."
          % (res.status_code, end, (end-start).total_seconds()))
    if res.status_code != 200:
        raise ValueError(res.status_code)
    return json.loads(res.get_data())


def test_health():
    res = _call_api('get', 'health')
    assert res == {'state': 'healthy', 'version': '1.0.0'}


@patch('indra_world.sources.dart.client.get_content_by_storage_key')
def test_notify(mock_get):
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    # Configure the mock to return a response with an OK status code.
    mock_get.return_value = _get_eidos_output()

    # Call the service, which will send a request to the server.
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key
                    ))
    assert res
    records = sc.db.get_dart_records(
        reader='eidos',
        document_id=doc_id,
        reader_version='1.0')
    assert records == [storage_key], records

    stmts = sc.db.get_statements_for_document(document_id=doc_id)
    assert len(stmts) == 1, stmts


@patch('indra_world.sources.dart.client.get_content_by_storage_key')
@raises(ValueError)
def test_notify_duplicate(mock_get):
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    # Configure the mock to return a response with an OK status code.
    mock_get.return_value = _get_eidos_output()

    # Call the service, which will send a request to the server.
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key
                    ))
    assert res
    res = _call_api('post', 'dart/notify',
                    json=dict(
                        identity='eidos',
                        version='1.0',
                        document_id=doc_id,
                        storage_key=storage_key
                    ))


def test_get_projects():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))
    res = _call_api('get', 'assembly/get_projects', json={})
    assert res


@patch('indra_world.sources.dart.client.get_content_by_storage_key')
def test_get_project_records(mock_get):
    mock_get.return_value = _get_eidos_output()
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()
    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))
    doc_id = '70a62e43-f881-47b1-8367-a3cca9450c03'
    storage_key = 'bcd04c45-3cfc-456f-a31e-59e875aefabf.json'
    record = {'identity': 'eidos',
              'version': '1.0',
              'document_id': doc_id,
              'storage_key': storage_key}
    res = _call_api('post', 'dart/notify', json=record)
    res = _call_api('post', 'assembly/add_project_records',
                    json=dict(
                        project_id='p1',
                        records=[record]
                    ))
    res = _call_api('get', 'assembly/get_project_records',
                    json=dict(project_id='p1'))
    assert res == [storage_key]


def test_curations():
    sc.db = DbManager(url='sqlite:///:memory:')
    sc.db.create_all()

    _call_api('post', 'assembly/new_project',
              json=dict(
                  project_id='p1',
                  project_name='Project 1'
              ))
    curation = {'project_id': 'p1',
                'statement_id': '12345',
                'update_type': 'discard_statement'}
    _call_api('post', 'assembly/submit_curations',
              json=dict(
                  project_id='p1',
                  curations={'12345': curation}
              ))
    res = _call_api('get', 'assembly/get_project_curations',
                    json=dict(project_id='p1'))
    assert len(res) == 1
    assert res[0] == curation
