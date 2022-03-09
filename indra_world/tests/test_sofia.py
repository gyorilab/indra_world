import os
import unittest
from nose.plugins.attrib import attr

from indra_world.sources import sofia
from indra_world.sources.sofia.processor import SofiaProcessor
from indra.statements.statements import Influence, Event
from indra.statements.context import WorldContext


# Tell nose to not run tests in the imported modules
Influence.__test__ = False
Event.__test__ = False
WorldContext.__test__ = False


path_here = os.path.abspath(os.path.dirname(__file__))


def _get_data_file(fname):
    return os.path.join(path_here, 'data', 'sofia', fname)


@attr('webservice', 'nonpublic')
@unittest.skip('webservice non-responsive')
def test_text_process_webservice():
    txt = 'rainfall causes floods'
    sp = sofia.process_text(txt)
    assert len(sp.statements) == 1
    assert sp.statements[0].subj.concept.name == 'rainfall'
    assert sp.statements[0].obj.concept.name == 'floods'


def test_process_json():
    test_file = _get_data_file('sofia_test.json')
    sp = sofia.process_json_file(test_file, grounding_mode='flat')
    assert len(sp.statements) == 2
    assert isinstance(sp.statements[0], Influence)
    assert sp.statements[0].subj.concept.name == 'rainfall'
    assert sp.statements[0].obj.concept.name == 'floods'
    assert len(sp.statements[0].evidence) == 1, len(sp.statements[0].evidence)
    assert isinstance(sp.statements[1], Event)
    assert sp.statements[1].concept.name == 'inflation'
    assert isinstance(sp.statements[1].context, WorldContext)
    assert sp.statements[1].context.time.text == '28, JULY, 2016'
    assert sp.statements[1].context.geo_location.name == 'South Sudan'


def test_compositional_grounding():
    test_file = _get_data_file('sofia_test_comp_no_causal.json')
    sp = sofia.process_json_file(test_file, grounding_mode='compositional')
    assert len(sp.statements) == 2
    assert isinstance(sp.statements[0], Event)
    assert isinstance(sp.statements[1], Event)

    assert sp.statements[0].concept.db_refs['TEXT'] == 'wheat', \
        sp.statements[0].concept.db_refs
    assert isinstance(sp.statements[0].concept.db_refs['WM'], list)
    assert isinstance(sp.statements[0].concept.db_refs['WM'][0], tuple), \
        sp.statements[0].concept.db_refs['WM']

    assert sp.statements[1].concept.db_refs['TEXT'] == 'cereal'
    assert isinstance(sp.statements[1].concept.db_refs['WM'], list)
    assert isinstance(sp.statements[1].concept.db_refs['WM'][0], tuple)


def test_event_decrease():
    test_file = _get_data_file('sofia_event_decreased.json')
    sp = sofia.process_json_file(test_file, grounding_mode='flat')
    assert len(sp.statements) == 1, sp.statements
    stmt = sp.statements[0]
    assert isinstance(stmt, Event), stmt
    assert stmt.delta.polarity == -1, stmt.delta
    assert stmt.concept.name == 'rainfall', stmt.concept

    # Test extraction filter
    sp = sofia.process_json_file(test_file, extract_filter={'influence'},
                                 grounding_mode='flat')
    assert len(sp.statements) == 0


def test_influence_event_polarity():
    test_file = _get_data_file('sofia_infl_polarities.json')
    sp = sofia.process_json_file(test_file, grounding_mode='flat')
    assert len(sp.statements) == 1, sp.statements
    stmt = sp.statements[0]
    assert isinstance(stmt, Influence)
    assert stmt.subj.delta.polarity == 1, stmt.subj.delta
    assert stmt.obj.delta.polarity == -1, stmt.obj.delta


def test_grounding_normalize():
    grnd, _ = SofiaProcessor._clean_grnd_filter(
        'event/base_path/concept/plan', 0.6, 'process')
    assert grnd == 'wm/concept/plan', grnd
    grnd, _ = SofiaProcessor._clean_grnd_filter(
        'event/base_path/process/provision', 0.6, 'process')
    assert grnd == 'wm/process/provision', grnd
