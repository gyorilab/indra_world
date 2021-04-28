import logging
import datetime
from indra.statements import Event, QualitativeDelta, WorldContext, \
    TimeContext, RefContext
from indra.sources.eidos.processor import EidosProcessor, EidosDocument


logger = logging.getLogger(__name__)


class EidosWorldProcessor(EidosProcessor):
    def __init__(self, json_dict, grounding_ns):
        super().__init__(json_dict=json_dict)
        self.doc = EidosWorldDocument(json_dict)
        self.statements = []
        self.grounding_ns = grounding_ns

    def get_event(self, event):
        concept = self.get_concept(event)
        states = event.get('states', [])
        extracted_states = self.extract_entity_states(states)
        polarity = extracted_states.get('polarity')
        adjectives = extracted_states.get('adjectives')
        delta = QualitativeDelta(polarity=polarity, adjectives=adjectives)
        timex = extracted_states.get('time_context', None)
        geo = extracted_states.get('geo_context', None)
        context = WorldContext(time=timex, geo_location=geo) \
            if timex or geo else None
        stmt = Event(concept, delta=delta, context=context)
        return stmt

    def get_groundings(self, entity):
        """Return groundings as db_refs for an entity."""
        def get_grounding_entries(grounding):
            if not grounding:
                return None

            entries = []
            values = grounding.get('values', [])
            # Values could still have been a None entry here
            if values:
                for entry in values:
                    ont_concept = entry.get('ontologyConcept')
                    value = entry.get('value')
                    if ont_concept is None or value is None:
                        continue
                    entries.append((ont_concept, value))
            return entries

        # Save raw text and Eidos scored groundings as db_refs
        db_refs = {'TEXT': entity['text']}
        groundings = entity.get('groundings')
        if not groundings:
            return db_refs
        for g in groundings:
            entries = get_grounding_entries(g)
            # Only add these groundings if there are actual values listed
            if entries:
                key = g['name'].upper()
                if self.grounding_ns is not None and \
                        key not in self.grounding_ns:
                    continue
                if key == 'UN':
                    db_refs[key] = [(s[0].replace(' ', '_'), s[1])
                                    for s in entries]
                elif key == 'WM_FLATTENED' or key == 'WM':
                    db_refs['WM'] = [(s[0].strip('/'), s[1])
                                     for s in entries]
                else:
                    db_refs[key] = entries
        return db_refs

    def time_context_from_ref(self, timex):
        """Return a time context object given a timex reference entry."""
        # If the timex has a value set, it means that it refers to a DCT or
        # a TimeExpression e.g. "value": {"@id": "_:DCT_1"} and the parameters
        # need to be taken from there
        value = timex.get('value')
        if value:
            # Here we get the TimeContext directly from the stashed DCT
            # dictionary
            tc = self.doc.timexes.get(value['@id'])
            return tc
        return None

    def geo_context_from_ref(self, ref):
        """Return a ref context object given a location reference entry."""
        value = ref.get('value')
        if value:
            # Here we get the RefContext from the stashed geoloc dictionary
            rc = self.doc.geolocs.get(value['@id'])
            return rc
        return None

    def extract_entity_states(self, states):
        states_processed = super().extract_entity_states(states)
        states_processed.update(self.extract_entity_time_loc_states(states))
        return states_processed

    def extract_entity_time_loc_states(self, states):
        if states is None:
            return {'time_context': None, 'geo_context': None}
        time_context = None
        geo_context = None
        for state in states:
            if state['type'] == 'TIMEX':
                time_context = self.time_context_from_ref(state)
            elif state['type'] == 'LocationExp':
                # TODO: here we take only the first geo_context occurrence.
                # Eidos sometimes provides a list of locations, it may
                # make sense to break those up into multiple statements
                # each with one location
                if not geo_context:
                    geo_context = self.geo_context_from_ref(state)
        return {'time_context': time_context, 'geo_context': geo_context}


class EidosWorldDocument(EidosDocument):
    def __init__(self, json_dict):
        self.timexes = {}
        self.geolocs = {}
        super().__init__(json_dict)

    def _preprocess_extractions(self):
        super()._preprocess_extractions()

        # Build a dictionary of sentences and document creation times (DCTs)
        documents = self.tree.execute("$.documents[(@.@type is 'Document')]")
        for document in documents:
            dct = document.get('dct')
            # We stash the DCT here as a TimeContext object
            if dct is not None:
                self.dct = self.time_context_from_dct(dct)
                self.timexes[dct['@id']] = self.dct
            sentences = document.get('sentences', [])
            for sent in sentences:
                timexes = sent.get('timexes')
                if timexes:
                    for timex in timexes:
                        tc = time_context_from_timex(timex)
                        self.timexes[timex['@id']] = tc
                geolocs = sent.get('geolocs')
                if geolocs:
                    for geoloc in geolocs:
                        rc = ref_context_from_geoloc(geoloc)
                        self.geolocs[geoloc['@id']] = rc

    @staticmethod
    def time_context_from_dct(dct):
        """Return a time context object given a DCT entry."""
        time_text = dct.get('text')
        start = _get_time_stamp(dct.get('start'))
        end = _get_time_stamp(dct.get('end'))
        duration = _get_duration(start, end)
        tc = TimeContext(text=time_text, start=start, end=end,
                         duration=duration)
        return tc


class EidosProcessorCompositional(EidosWorldProcessor):
    def get_groundings(self, entity):
        """Return groundings as db_refs for an entity."""
        def get_grounding_entries_comp(grounding):
            if not grounding:
                return None

            entry_types = ['theme', 'themeProperties', 'themeProcess',
                           'themeProcessProperties']
            entries = []
            values = grounding.get('values', [])
            # Values could still have been a None entry here
            if values:
                for entry in values:
                    compositional_entry = [None, None, None, None]
                    for idx, entry_type in enumerate(entry_types):
                        val = entry.get(entry_type)
                        if val is None:
                            continue
                        # FIXME: can there be multiple entries here?
                        val = val[0]
                        ont_concept = val.get('ontologyConcept')
                        score = val.get('value')
                        if ont_concept is None or score is None:
                            continue
                        if ont_concept.endswith('/'):
                            ont_concept = ont_concept[:-1]
                        compositional_entry[idx] = \
                            (ont_concept, score)
                    # Some special cases
                    # Promote process into theme
                    if compositional_entry[2] and not compositional_entry[0]:
                        compositional_entry[0] = compositional_entry[2]
                        compositional_entry[2] = None
                        if compositional_entry[3]:
                            compositional_entry[1] = compositional_entry[3]
                            compositional_entry[3] = None
                    # Promote dangling property
                    if compositional_entry[1] and not compositional_entry[0]:
                        compositional_entry[0] = compositional_entry[1]
                        compositional_entry[1] = None
                    # Promote theme process property into theme property
                    if compositional_entry[3] and not compositional_entry[2] \
                            and not compositional_entry[1]:
                        compositional_entry[1] = compositional_entry[3]
                        compositional_entry[3] = None
                    if any(compositional_entry):
                        entries.append(compositional_entry)
            return entries

        # Save raw text and Eidos scored groundings as db_refs
        db_refs = {'TEXT': entity['text']}
        groundings = entity.get('groundings')
        if not groundings:
            return db_refs
        for g in groundings:
            key = g['name'].upper()
            if key == 'WM_COMPOSITIONAL':
                entries = get_grounding_entries_comp(g)
                if entries:
                    db_refs['WM'] = entries
            else:
                continue
        return db_refs


def _get_time_stamp(entry):
    """Return datetime object from a timex constraint start/end entry.

    Example string format to convert: 2018-01-01T00:00
    """
    if not entry or entry == 'Undef':
        return None
    try:
        dt = datetime.datetime.strptime(entry, '%Y-%m-%dT%H:%M')
    except Exception as e:
        logger.debug('Could not parse %s format' % entry)
        return None
    return dt


def _get_duration(start, end):
    if not start or not end:
        return None
    try:
        duration = int((end - start).total_seconds())
    except Exception as e:
        logger.debug('Failed to get duration from %s and %s' %
                     (str(start), str(end)))
        duration = None
    return duration


def ref_context_from_geoloc(geoloc):
    """Return a RefContext object given a geoloc entry."""
    text = geoloc.get('text')
    geoid = geoloc.get('geoID')
    rc = RefContext(name=text, db_refs={'GEOID': geoid})
    return rc


def time_context_from_timex(timex):
    """Return a TimeContext object given a timex entry."""
    time_text = timex.get('text')
    intervals = timex.get('intervals')
    if not intervals:
        start = end = duration = None
    else:
        constraint = intervals[0]
        start = _get_time_stamp(constraint.get('start'))
        end = _get_time_stamp(constraint.get('end'))
        duration = _get_duration(start, end)
    tc = TimeContext(text=time_text, start=start, end=end,
                     duration=duration)
    return tc


