__all__ = ['has_location', 'has_time', 'get_location_from_object',
           'get_location', 'get_time', 'has_delta', 'get_delta',
           'concept_matches_compositional', 'matches_compositional',
           'location_matches_compositional', 'location_matches',
           'event_location_time_matches', 'event_location_time_delta_matches',
           'location_time_delta_matches']
from indra.statements import Influence, Event, Migration, QuantitativeState, \
    QualitativeDelta
from indra.pipeline import register_pipeline


def has_location(stmt):
    """Return True if a Statement has grounded geo-location context."""
    if isinstance(stmt, Migration):
        if not stmt.context or not stmt.context.locations:
            return False
    elif not stmt.context or not stmt.context.geo_location or \
            not (stmt.context.geo_location.db_refs.get('GEOID') or
                 stmt.context.geo_location.name):
        return False
    return True


def has_time(stmt):
    """Return True if a Statement has time context."""
    if not stmt.context or not stmt.context.time:
        return False
    return True


def get_location_from_object(loc_obj):
    """Return geo-location from a RefContext location object."""
    if loc_obj.db_refs.get('GEOID'):
        return loc_obj.db_refs['GEOID']
    elif loc_obj.name:
        return loc_obj.name
    else:
        return None


def get_location(stmt):
    """Return the grounded geo-location context associated with a Statement."""
    if not has_location(stmt):
        location = None
    elif isinstance(stmt, Migration):
        location = []
        for loc in stmt.context.locations:
            loc_obj = loc['location']
            location.append((get_location_from_object(loc_obj), loc['role']))
    else:
        location = get_location_from_object(stmt.context.geo_location)
    return location


def get_time(stmt):
    """Return the time context associated with a Statement."""
    if not has_time(stmt):
        time = None
    else:
        time = stmt.context.time
    return time


def has_delta(stmt):
    if not stmt.delta:
        return False
    return True


def get_delta(stmt):
    delta = stmt.delta
    if isinstance(delta, QualitativeDelta):
        return delta.polarity
    elif isinstance(delta, QuantitativeState):
        return (delta.entity, delta.value, delta.unit, delta.polarity)


@register_pipeline
def concept_matches_compositional(concept):
    wm = concept.db_refs.get('WM')
    if not wm:
        return concept.name
    wm_top = tuple(entry[0] if entry else None for entry in wm[0])
    return wm_top


@register_pipeline
def matches_compositional(stmt):
    if isinstance(stmt, Influence):
        key = (stmt.__class__.__name__,
               concept_matches_compositional(stmt.subj.concept),
               concept_matches_compositional(stmt.obj.concept),
               stmt.polarity_count(),
               stmt.overall_polarity()
               )
    elif isinstance(stmt, Event):
        key = (stmt.__class__.__name__,
               concept_matches_compositional(stmt.concept),
               stmt.delta.polarity)
    # TODO: handle Associations?
    return str(key)


@register_pipeline
def location_matches_compositional(stmt):
    """Return a matches_key which takes geo-location into account."""
    if isinstance(stmt, Event):
        context_key = get_location(stmt)
        matches_key = str((matches_compositional(stmt), context_key))
    elif isinstance(stmt, Influence):
        subj_context_key = get_location(stmt.subj)
        obj_context_key = get_location(stmt.obj)
        matches_key = str((matches_compositional(stmt), subj_context_key,
                           obj_context_key))
    else:
        matches_key = matches_compositional(stmt)
    return matches_key


@register_pipeline
def location_matches(stmt):
    """Return a matches_key which takes geo-location into account."""
    if isinstance(stmt, Event):
        context_key = get_location(stmt)
        matches_key = str((stmt.concept.matches_key(), context_key))
    elif isinstance(stmt, Influence):
        subj_context_key = get_location(stmt.subj)
        obj_context_key = get_location(stmt.obj)
        matches_key = str((stmt.matches_key(), subj_context_key,
                           obj_context_key))
    else:
        matches_key = stmt.matches_key()
    return matches_key


@register_pipeline
def event_location_time_matches(event):
    """Return Event matches key which takes location and time into account."""
    mk = location_matches(event)
    if not has_time(event):
        return mk
    time = get_time(event)
    matches_key = str((mk, time.start, time.end, time.duration))
    return matches_key


@register_pipeline
def location_time_matches(stmt):
    """Return matches key which takes location and time into account."""
    if isinstance(stmt, Event):
        return event_location_time_matches(stmt)
    elif isinstance(stmt, Influence):
        subj_mk = event_location_time_matches(stmt.subj)
        obj_mk = event_location_time_matches(stmt.obj)
        return str((stmt.matches_key(), subj_mk, obj_mk))
    else:
        return stmt.matches_key()


@register_pipeline
def event_location_time_delta_matches(event):
    mk = event_location_time_matches(event)
    if not has_delta:
        return mk
    delta = get_delta(event)
    matches_key = str((mk, delta))
    return matches_key


@register_pipeline
def location_time_delta_matches(stmt):
    if isinstance(stmt, Event):
        return event_location_time_delta_matches(stmt)
    elif isinstance(stmt, Influence):
        subj_mk = event_location_time_delta_matches(stmt.subj)
        obj_mk = event_location_time_delta_matches(stmt.obj)
        return str((stmt.matches_key(), subj_mk, obj_mk))
    else:
        return stmt.matches_key()


