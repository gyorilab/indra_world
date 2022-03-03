__all__ = ['process_text', 'process_json', 'process_json_str',
           'process_json_file', 'reground_texts']

import json
import logging
from indra_world.sources.eidos import client as eidos_client
from indra.sources.eidos.api import eidos_reader, _run_eidos_on_text
from .processor import EidosWorldProcessor, EidosProcessorCompositional

logger = logging.getLogger(__name__)


default_grounding_mode = 'compositional'  # The alternative is 'compositional'


def process_text(text, save_json='eidos_output.json',
                 webservice=None, grounding_ns=None, extract_filter=None,
                 grounding_mode=default_grounding_mode):
    """Return an EidosProcessor by processing the given text.

    This constructs a reader object via Java and extracts mentions
    from the text. It then serializes the mentions into JSON and
    processes the result with process_json.

    Parameters
    ----------
    text : str
        The text to be processed.
    save_json : Optional[str]
        The name of a file in which to dump the JSON output of Eidos.
    webservice : Optional[str]
        An Eidos reader web service URL to send the request to.
        If None, the reading is assumed to be done with the Eidos JAR rather
        than via a web service. Default: None
    grounding_ns : Optional[list]
        A list of name spaces for which INDRA should represent groundings, when
        given. If not specified or None, all grounding name spaces are
        propagated. If an empty list, no groundings are propagated.
        Example: ['UN', 'WM'], Default: None
    extract_filter : Optional[list]
        A list of relation types to extract. Valid values in the list are
        'influence', 'association', 'event'. If not given, all relation
        types are extracted. This argument can be used if, for instance,
        only Influence statements are of interest. Default: None
    grounding_mode : Optional[str]
        Selects whether 'flat' or 'compositional' groundings should be
        extracted. Default: 'flat'.

    Returns
    -------
    ep : EidosProcessor
        An EidosProcessor containing the extracted INDRA Statements in its
        statements attribute.
    """
    json_dict = _run_eidos_on_text(text, save_json, webservice)
    if json_dict:
        return process_json(json_dict, grounding_ns=grounding_ns,
                            extract_filter=extract_filter,
                            grounding_mode=grounding_mode)
    return None


def process_json_file(file_name, grounding_ns=None, extract_filter=None,
                      grounding_mode=default_grounding_mode):
    """Return an EidosProcessor by processing the given Eidos JSON-LD file.

    This function is useful if the output from Eidos is saved as a file and
    needs to be processed.

    Parameters
    ----------
    file_name : str
        The name of the JSON-LD file to be processed.
    grounding_ns : Optional[list]
        A list of name spaces for which INDRA should represent groundings, when
        given. If not specified or None, all grounding name spaces are
        propagated. If an empty list, no groundings are propagated.
        Example: ['UN', 'WM'], Default: None
    extract_filter : Optional[list]
        A list of relation types to extract. Valid values in the list are
        'influence', 'association', 'event'. If not given, all relation
        types are extracted. This argument can be used if, for instance,
        only Influence statements are of interest. Default: None
    grounding_mode : Optional[str]
        Selects whether 'flat' or 'compositional' groundings should be
        extracted. Default: 'flat'.

    Returns
    -------
    ep : EidosProcessor
        A EidosProcessor containing the extracted INDRA Statements
        in its statements attribute.
    """
    try:
        with open(file_name, 'rb') as fh:
            json_str = fh.read().decode('utf-8')
            return process_json_str(json_str, grounding_ns=grounding_ns,
                                    extract_filter=extract_filter,
                                    grounding_mode=grounding_mode)
    except IOError:
        logger.exception('Could not read file %s.' % file_name)


def process_json_str(json_str, grounding_ns=None, extract_filter=None,
                     grounding_mode=default_grounding_mode):
    """Return an EidosProcessor by processing the Eidos JSON-LD string.

    Parameters
    ----------
    json_str : str
        The JSON-LD string to be processed.
    grounding_ns : Optional[list]
        A list of name spaces for which INDRA should represent groundings, when
        given. If not specified or None, all grounding name spaces are
        propagated. If an empty list, no groundings are propagated.
        Example: ['UN', 'WM'], Default: None
    extract_filter : Optional[list]
        A list of relation types to extract. Valid values in the list are
        'influence', 'association', 'event'. If not given, all relation
        types are extracted. This argument can be used if, for instance,
        only Influence statements are of interest. Default: None
    grounding_mode : Optional[str]
        Selects whether 'flat' or 'compositional' groundings should be
        extracted. Default: 'flat'.

    Returns
    -------
    ep : EidosProcessor
        A EidosProcessor containing the extracted INDRA Statements
        in its statements attribute.
    """
    json_dict = json.loads(json_str)
    return process_json(json_dict, grounding_ns=grounding_ns,
                        extract_filter=extract_filter,
                        grounding_mode=grounding_mode)


def process_json(json_dict, grounding_ns=None, extract_filter=None,
                 grounding_mode=None):
    """Return an EidosProcessor by processing a Eidos JSON-LD dict.

    Parameters
    ----------
    json_dict : dict
        The JSON-LD dict to be processed.
    grounding_ns : Optional[list]
        A list of name spaces for which INDRA should represent groundings, when
        given. If not specified or None, all grounding name spaces are
        propagated. If an empty list, no groundings are propagated.
        Example: ['UN', 'WM'], Default: None
    extract_filter : Optional[list]
        A list of relation types to extract. Valid values in the list are
        'influence', 'association', 'event'. If not given, all relation
        types are extracted. This argument can be used if, for instance,
        only Influence statements are of interest. Default: None
    grounding_mode : Optional[str]
        Selects whether 'flat' or 'compositional' groundings should be
        extracted. Default: 'flat'.

    Returns
    -------
    ep : EidosProcessor
        A EidosProcessor containing the extracted INDRA Statements
        in its statements attribute.
    """
    grounding_mode = default_grounding_mode if not grounding_mode \
        else grounding_mode
    if grounding_mode == 'flat':
        ep = EidosWorldProcessor(json_dict, grounding_ns=grounding_ns)
    elif grounding_mode == 'compositional':
        ep = EidosProcessorCompositional(json_dict, grounding_ns=grounding_ns)
    else:
        raise ValueError('Invalid grounding mode: %s' % grounding_mode)

    if extract_filter is None or 'influence' in extract_filter:
        ep.extract_causal_relations()
    if extract_filter is None or 'association' in extract_filter:
        ep.extract_correlations()
    if extract_filter is None or 'event' in extract_filter:
        ep.extract_events()
    return ep


def reground_texts(texts, ont_yml, webservice=None, topk=10, filter=True,
                   is_canonicalized=True):
    """Return grounding for concept texts given an ontology.

    Parameters
    ----------
    texts : list[str]
        A list of concept texts to ground.
    ont_yml : str
        A serialized YAML string representing the ontology.
    webservice : Optional[str]
        The address where the Eidos web service is running, e.g.,
        http://localhost:9000. If None, a local Eidos JAR is invoked
        via pyjnius. Default: None
    topk : Optional[int]
        The number of top scoring groundings to return. Default: 10
    is_canonicalized : Optional[bool]
        If True, the texts are assumed to be canonicalized. If False,
        Eidos will canonicalize the texts which yields much better groundings
        but is slower. Default: False
    filter : Optional[bool]
        If True, Eidos filters the ontology to remove determiners from examples
        and other similar operations. Should typically be set to True.
        Default: True

    Returns
    -------
    list[list]
        A list of the top k scored groundings for each text in the list.
    """
    if not webservice:
        return eidos_reader.reground_texts(texts, ont_yml, topk=topk,
                                           filter=filter,
                                           is_canonicalized=is_canonicalized)
    else:
        return eidos_client.reground_texts(texts, ont_yml, webservice,
                                           topk=topk, filter=filter,
                                           is_canonicalized=is_canonicalized)
