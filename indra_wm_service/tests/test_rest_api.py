import json
from datetime import datetime
from unittest.mock import patch
from indra_wm_service.app import api
from indra_wm_service.app import sc
from indra_wm_service.db.manager import DbManager
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
    assert res.status_code == 200, res.status_code
    return json.loads(res.get_data())


def test_health():
    res = _call_api('get', 'health')
    assert res == {'state': 'healthy', 'version': '1.0.0'}


@patch('indra.literature.dart_client.get_content_by_storage_key')
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
    record = sc.db.get_dart_record(
        reader='eidos',
        document_id=doc_id,
        reader_version='1.0')
    assert record == [storage_key], record

    stmts = sc.db.get_statements_for_document(document_id=doc_id)
    assert len(stmts) == 1, stmts

