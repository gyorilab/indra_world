import json
import copy
import logging
import unittest
from nose.plugins.attrib import attr
from indra.statements import *
from indra_wm_service.live_curation import app, curator
from indra_wm_service.corpus import Corpus
from indra_wm_service.curator import LiveCurator

logger = logging.getLogger(__name__)


def _make_corpus():
    ev1 = Evidence(source_api='eidos', text='A',
                   annotations={'found_by': 'ported_syntax_1_verb-Causal'})
    ev2 = Evidence(source_api='eidos', text='B',
                   annotations={'found_by': 'dueToSyntax2-Causal'})
    ev3 = Evidence(source_api='hume', text='C')
    ev4 = Evidence(source_api='cwms', text='D')
    ev5 = Evidence(source_api='sofia', text='E')
    ev6 = Evidence(source_api='sofia', text='F')
    x = Event(Concept('x', db_refs={'TEXT': 'dog'}))
    y = Event(Concept('y', db_refs={'TEXT': 'cat'}))
    stmt1 = Influence(x, y, evidence=[ev1, ev2])
    stmt2 = Influence(x, y, evidence=[ev1, ev3])
    stmt3 = Influence(x, y, evidence=[ev3, ev4, ev5])
    stmt4 = Influence(x, y, evidence=[ev5])
    stmt5 = Influence(x, y, evidence=[ev6])
    stmt1.uuid = '1'
    stmt2.uuid = '2'
    stmt3.uuid = '3'
    stmt4.uuid = '4'
    stmt5.uuid = '5'
    stmts = [stmt1, stmt2, stmt3, stmt4, stmt5]
    raw_stmts = copy.deepcopy(stmts)
    return Corpus(corpus_id='x', statements=stmts, raw_statements=raw_stmts)


def test_no_curation():
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curator.submit_curations(curations=[])
    beliefs = curator.update_beliefs(corpus_id='1')
    expected = {'1': 0.91675,
                '2': 0.8968,
                '3': 0.957125,
                '4': 0.65,
                '5': 0.65}
    assert close_enough(beliefs, expected), (beliefs, expected)


def test_eid_rule1_incorrect():
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curations = [{'corpus_id': '1',
                  'statement_id': '1',
                  'update_type': 'discard_statement'}]
    curator.submit_curations(curations=curations)
    expected = {'1': 0,
                '2': 0.8942,
                '3': 0.957125,
                '4': 0.65,
                '5': 0.65}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)

    # Submit another curation
    curator.submit_curations(curations=curations)
    expected = {'1': 0,
                '2': 0.8917,
                '3': 0.957125,
                '4': 0.65,
                '5': 0.65}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)


def test_eid_rule1_correct():
    curations = [{'corpus_id': '1',
                  'statement_id': '1',
                  'update_type': 'vet_statement'}]
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curator.submit_curations(curations=curations)
    expected = {'1': 1,
                '2': 0.8979,
                '3': 0.957125,
                '4': 0.65,
                '5': 0.65}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)


def test_eid_rule2_correct():
    curations = [{'corpus_id': '1',
                  'statement_id': '2',
                  'update_type': 'vet_statement'}]
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curator.submit_curations(curations=curations)
    expected = {'1': 0.91717,
                '2': 1,
                '3': 0.95916,
                '4': 0.65,
                '5': 0.65}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)


def test_hume_incorrect():
    curations = [{'corpus_id': '1',
                  'statement_id': '3',
                  'update_type': 'discard_statement'}]
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curator.submit_curations(curations=curations)
    expected = {'1': 0.91675,
                '2': 0.88772,
                '3': 0,
                '4': 0.61904,
                '5': 0.61904}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)


def test_sofia_incorrect():
    curations = [{'corpus_id': '1',
                  'statement_id': '4',
                  'update_type': 'discard_statement'}]
    curator = LiveCurator(corpora={'1': _make_corpus()})
    curator.submit_curations(curations=curations)
    expected = {'1': 0.91675,
                '2': 0.89684,
                '3': 0.9533,
                '4': 0.0,
                '5': 0.61904}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)

    curations = [{'corpus_id': '1',
                  'statement_id': '5',
                  'update_type': 'discard_statement'}]
    curator.submit_curations(curations=curations)
    expected = {'1': 0.91675,
                '2': 0.89684,
                '3': 0.9533,
                '4': 0,
                '5': 0}
    beliefs = curator.update_beliefs(corpus_id='1')
    assert close_enough(beliefs, expected), (beliefs, expected)


class LiveCurationTestCase(unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.app = app.test_client()
        curator.corpora = {'1': _make_corpus()}

    def _send_request(self, endpoint, req_dict):
        resp = self.app.post(endpoint,
                             data=json.dumps(req_dict),
                             headers={'Content-Type': 'application/json'})
        return resp

    def _reset_scorer(self):
        resp = self.app.post('reset_curations',
                             data='{}',
                             headers={'Content-Type': 'application/json'})
        assert resp.status_code == 200, resp

    # Tests ==================
    def test_alive(self):
        resp = self._send_request('health', {})
        assert resp.status_code == 200, resp

    def test_bad_corpus(self):
        curs = {'curations': [
            {'corpus_id': 'xxxx'}
        ]}
        resp = self._send_request('submit_curations', curs)
        assert resp.status_code == 400, resp

    def test_no_curation(self):
        curs = {'curations': []}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0.91675,
                    '2': 0.8968,
                    '3': 0.957125,
                    '4': 0.65,
                    '5': 0.65}
        assert close_enough(res, expected), (res, expected)

    def test_eid_rule1_incorrect(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '1'}
        ]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0,
                    '2': 0.8942,
                    '3': 0.957125,
                    '4': 0.65,
                    '5': 0.65}
        assert close_enough(res, expected), (res, expected)

    def test_eid_rule1_incorrect_again(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '1'},
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '1'},
        ]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200, resp
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0,
                    '2': 0.88687,
                    '3': 0.957125,
                    '4': 0.65,
                    '5': 0.65}
        assert close_enough(res, expected), (res, expected)

    def test_eid_rule1_correct(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'vet_statement',
             'statement_id': '1'}]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 1,
                    '2': 0.8979,
                    '3': 0.957125,
                    '4': 0.65,
                    '5': 0.65}
        assert close_enough(res, expected), (res, expected)

    def test_eid_rule2_correct(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'vet_statement',
             'statement_id': '2'}]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0.91717,
                    '2': 1,
                    '3': 0.95916,
                    '4': 0.65,
                    '5': 0.65}
        assert close_enough(res, expected), (res, expected)

    def test_hume_incorrect(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '3'}]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0.91675,
                    '2': 0.88772,
                    '3': 0,
                    '4': 0.61904,
                    '5': 0.61904}
        assert close_enough(res, expected), (res, expected)

    def test_sofia_incorrect(self):
        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '4'}]}
        self._reset_scorer()
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0.91675,
                    '2': 0.89684,
                    '3': 0.9533,
                    '4': 0.0,
                    '5': 0.61904}
        assert close_enough(res, expected), (res, expected)

        curs = {'curations': [
            {'corpus_id': '1',
             'update_type': 'discard_statement',
             'statement_id': '5'}]}
        self._send_request('submit_curations', curs)
        resp = self._send_request('update_beliefs', {'corpus_id': '1'})
        assert resp.status_code == 200
        res = json.loads(resp.data.decode('utf-8'))
        expected = {'1': 0.91675,
                    '2': 0.89684,
                    '3': 0.9533,
                    '4': 0,
                    '5': 0}
        assert close_enough(res, expected), (res, expected)


@attr('notravis')
class LiveGroundingTestCase(unittest.TestCase):
    def _send_request(self, endpoint, req_dict):
        resp = self.app.post(endpoint,
                             data=json.dumps(req_dict),
                             headers={'Content-Type': 'application/json'})
        return resp

    def setUp(self):
        _make_corpus()
        app.testing = True
        self.app = app.test_client()
        curator.eidos_url = 'http://localhost:9000'
        curator.corpora = {'1': _make_corpus()}

    def test_add_ontology_node(self):
        self._send_request('add_ontology_entry',
                           {'entry': 'wm/animal/dog',
                            'examples': ['canine', 'dog', 'puppy']})
        resp = self._send_request('update_groundings', {'corpus_id': '1'})
        res = json.loads(resp.data.decode('utf-8'))
        stmts = stmts_from_json(res)
        assert stmts, stmts
        dr = stmts[0].subj.concept.db_refs
        assert 'WM' in dr, dr
        assert dr['WM'], dr
        assert dr['WM'][0][0] == 'wm/animal/dog', dr


def close_enough(probs, ref):
    for k, v in probs.items():
        if abs(ref[k] - probs[k]) > 0.0001:
            logger.error('%s: %.4f != %.4f' % (k, probs[k], ref[k]))
            return False
    return True

