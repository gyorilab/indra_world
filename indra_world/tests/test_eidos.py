import os
import json
import requests
import datetime
from nose.plugins.attrib import attr
from indra.sources.eidos.reader import EidosReader
from indra.statements import Influence, Event, Association
from indra.assemblers.pysb import PysbAssembler
from indra.assemblers.cx import CxAssembler
from indra_world.sources import eidos
from indra_world.assemblers.cag import CAGAssembler


path_here = os.path.dirname(os.path.abspath(__file__))


def _get_data_file(fname):
    return os.path.join(path_here, 'data', 'eidos', fname)


test_jsonld = _get_data_file('eidos_test.jsonld')


def _get_remote_jsonld():
    res = requests.get('https://raw.githubusercontent.com/clulab/eidos/master/'
                       'example_output/example-0.2.2.jsonld')
    assert res.status_code == 200, "Could not get example json from remote."
    example_json = json.loads(res.content.decode('utf-8'))
    return example_json


def _get_stmts_from_remote_jsonld():
    ex_json = _get_remote_jsonld()
    ep = eidos.process_json(ex_json)
    assert ep is not None, 'Failed to handle json with eidos processor.'
    assert len(ep.statements), 'Did not get statements from json.'
    return ep.statements


def test_process_text():
    ep = eidos.process_text('The cost of fuel decreases water trucking.')
    assert ep is not None
    assert len(ep.statements) == 1
    stmt = ep.statements[0]
    assert isinstance(stmt, Influence)
    assert stmt.subj.concept.name == 'fuel', stmt.subj.concept.name
    assert stmt.obj.concept.name == 'water trucking', stmt.obj.concept.name
    assert stmt.obj.delta.polarity == -1
    assert stmt.evidence[0].annotations['found_by'] == \
           'ported_syntax_1_verb-Causal'
    assert 'TEXT' in stmt.subj.concept.db_refs
    assert 'TEXT' in stmt.obj.concept.db_refs
    # NOTE: groundings are turned off in Travis tests so these are commented
    # out
    # assert 'UN' in stmt.subj.db_refs
    # assert 'UN' in stmt.obj.db_refs
    # assert len(stmt.subj.db_refs['UN']) > 5
    # assert len(stmt.obj.db_refs['UN']) > 5


def test_process_polarity():
    test_jsonld = _get_data_file('eidos_neg_event.json')
    ep = eidos.process_json_file(test_jsonld)
    assert ep is not None
    assert len(ep.statements) == 1
    stmt = ep.statements[0]
    assert isinstance(stmt, Influence)
    assert stmt.subj.concept.name == 'fuel', stmt.subj.concept.name
    assert stmt.obj.concept.name == 'water trucking', stmt.obj.concept.name
    assert stmt.obj.delta.polarity == -1
    assert stmt.evidence[0].annotations['found_by'] == \
           'ported_syntax_1_verb-Causal'
    assert 'TEXT' in stmt.subj.concept.db_refs
    assert 'TEXT' in stmt.obj.concept.db_refs


def test_process_json_ld_file():
    ep = eidos.process_json_file(test_jsonld, grounding_mode='flat')
    assert len(ep.statements) == 1
    st = ep.statements[0]
    assert 'UN' in st.subj.concept.db_refs, st.subj.concept.db_refs
    assert 'UN' in st.obj.concept.db_refs, st.obj.concept.db_refs

    ep = eidos.process_json_file(test_jsonld, grounding_ns=['UN'],
                                 grounding_mode='flat')
    st = ep.statements[0]
    assert set(st.subj.concept.db_refs.keys()) == {'TEXT', 'UN'}, \
        st.subj.concept.db_refs


def test_process_corefs():
    coref_jsonld = _get_data_file('eidos_coref.json')
    ep = eidos.process_json_file(coref_jsonld)
    assert ep.doc.coreferences.get('_:Extraction_6') == '_:Extraction_4'
    assert len(ep.statements) == 2
    # Get summaru of subj/objs from statements
    concepts = [(s.subj.concept.name, s.obj.concept.name) for s in
                ep.statements]
    assert ('rainfall', 'flood') in concepts, concepts
    # This ensures that the coreference was successfully resolved
    assert ('flood', 'displacement') in concepts, concepts


def test_process_timex():
    timex_jsonld = _get_data_file('eidos_timex.json')
    ep = eidos.process_json_file(timex_jsonld)
    assert len(ep.statements) == 1
    ev = ep.statements[0].evidence[0]
    assert ev.context is None
    subjc = ep.statements[0].subj.context
    assert subjc.__repr__() == subjc.__str__()
    assert subjc.time.duration == 365 * 86400, subjc.time.duration
    assert subjc.time.start == \
           datetime.datetime(year=2018, month=1, day=1, hour=0, minute=0), \
        subjc.time.start
    assert subjc.time.end == \
           datetime.datetime(year=2019, month=1, day=1, hour=0, minute=0), \
        subjc.time.end


def test_process_correlations():
    correl_jsonld = _get_data_file('eidos_correlation.json')
    ep = eidos.process_json_file(correl_jsonld)
    assert len(ep.statements) == 1
    st = ep.statements[0]
    assert isinstance(st, Association)
    assert isinstance(st.members[0], Event)
    names = {m.concept.name for m in st.members}
    assert names == {'harvest', 'requirement'}, names

    # This is to check the extraction filter
    ep = eidos.process_json_file(correl_jsonld, extract_filter={'influence'})
    assert len(ep.statements) == 0


def test_process_negation_hedging():
    nh_jsonld = _get_data_file('eidos_neg_hedge.json')
    ep = eidos.process_json_file(nh_jsonld)
    assert len(ep.statements) == 1
    st = ep.statements[0]
    epi = st.evidence[0].epistemics
    assert epi.get('hedgings') == ['may'], epi
    assert epi.get('negated') is True, epi
    annot = st.evidence[0].annotations
    assert annot.get('negated_texts') == ['not']


def test_process_geoids():
    geo_jsonld = _get_data_file('eidos_geoid.json')
    ep = eidos.process_json_file(geo_jsonld)
    # Make sure we collect all geoids up front
    ss_loc = {'name': 'South Sudan', 'db_refs': {'GEOID': '7909807'}}
    assert len(ep.doc.geolocs) == 1, len(ep.doc.geolocs)
    assert ep.doc.geolocs['_:GeoLocation_1'].to_json() == ss_loc
    # Make sure this event has the right geoid
    assert isinstance(ep.statements[0], Influence)
    ev = ep.statements[0].evidence[0]
    assert not ev.context
    assert ep.statements[0].obj.context.geo_location.to_json() == ss_loc
    # And that the subject context is captured in annotations
    assert 'obj_context' in ev.annotations, ev.annotations
    assert ev.annotations['obj_context']['geo_location'] == ss_loc


def test_eidos_to_cag():
    stmts = _get_stmts_from_remote_jsonld()
    ca = CAGAssembler()

    # Make sure these don't error
    ca.add_statements(stmts)
    ca.make_model()
    ca.export_to_cytoscapejs()
    return


def test_compositional_grounding():
    jsonld = _get_data_file('eidos_compositional.jsonld')
    ep = eidos.process_json_file(jsonld, grounding_mode='compositional')
    assert ep.statements


def test_eidos_to_cx():
    stmts = _get_stmts_from_remote_jsonld()
    cx = CxAssembler()

    # Make sure these don't error
    cx.add_statements(stmts)
    cx.make_model()
    test_fname = 'test_cag_to_cx.cx'
    try:
        cx.save_model(test_fname)
        assert os.path.exists(test_fname), "Failed to create cx file."
    finally:
        if os.path.exists(test_fname):
            os.remove(test_fname)
    return


# LibSBML used during model assembly causes out of memory error
@attr('notravis')
def test_eidos_to_pysb():
    stmts = _get_stmts_from_remote_jsonld()
    pa = PysbAssembler()

    # Make sure these don't error
    pa.add_statements(stmts)
    model = pa.make_model()
    assert model.rules, model.rules
    for fmt in ['kappa', 'sbml', 'sbgn']:
        exp_str = pa.export_model(fmt)
        assert exp_str, "Got no exported model from eidos->psyb to %s." % fmt
    return


def test_standalone_event():
    se_jsonld = _get_data_file('eidos_standalone_event.json')
    ep = eidos.process_json_file(se_jsonld)
    assert len(ep.statements) == 1
    st = ep.statements[0]
    assert isinstance(st, Event)
    assert hasattr(st, 'evidence')
    ev = st.evidence[0]
    assert ev.text is not None
    js = st.to_json()
    assert js['evidence']
    from indra.statements import stmts_to_json
    js2 = stmts_to_json([st])[0]
    assert 'evidence' in js2


def test_geoloc_obj():
    se_jsonld = _get_data_file('eidos_geoloc_obj.json')
    ep = eidos.process_json_file(se_jsonld)
    st = ep.statements[1]
    ev = st.evidence[0]
    assert not ev.context, ev.context
    assert st.obj.context


# Grounding not available on Travis.
@attr('notravis')
def test_reground_texts():
    er = EidosReader()
    er.initialize_reader()
    groundings = er.reground_texts(['rainfall', 'hunger'])
    assert groundings[0][0][0] == \
           ('wm/concept/causal_factor/environmental/'
            'meteorologic/precipitation/rainfall'), groundings
    assert groundings[1][0][0] == \
           'wm/concept/causal_factor/condition/famine', groundings
