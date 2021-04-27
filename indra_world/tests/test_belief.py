from indra.statements import Influence, Concept, Evidence
from indra.belief import BeliefEngine
from indra_world.belief import get_eidos_scorer


def test_wm_scorer():
    scorer = get_eidos_scorer()
    stmt = Influence(Concept('a'), Concept('b'),
                     evidence=[Evidence(source_api='eidos')])
    # Make sure other sources are still in the map
    assert 'hume' in scorer.prior_probs['rand']
    assert 'biopax' in scorer.prior_probs['syst']
    engine = BeliefEngine(scorer)
    engine.set_prior_probs([stmt])


