import yaml
import logging
from indra.belief.wm_scorer import get_eidos_bayesian_scorer
from indra.sources.eidos import reground_texts
from indra.tools import assemble_corpus as ac
from indra.belief import BeliefEngine
from . import file_defaults, default_key_base, InvalidCorpusError, CACHE
from .corpus import Corpus

logger = logging.getLogger(__name__)


class LiveCurator(object):
    """Class coordinating the real-time curation of a corpus of Statements.

    Parameters
    ----------
    scorer : indra.belief.BeliefScorer
        A scorer object to use for the curation
    corpora : dict[str, Corpus]
        A dictionary mapping corpus IDs to Corpus objects.
    """

    def __init__(self, scorer=None, corpora=None, eidos_url=None,
                 ont_manager=None, cache=CACHE):
        self.corpora = corpora if corpora else {}
        self.scorer = scorer if scorer else get_eidos_bayesian_scorer()
        self.ont_manager = ont_manager
        self.eidos_url = eidos_url
        self.cache = cache

    # TODO: generalize this to other kinds of scorers
    def reset_scorer(self):
        """Reset the scorer used for curation."""
        logger.info('Resetting the scorer')
        self.scorer = get_eidos_bayesian_scorer()
        for corpus_id, corpus in self.corpora.items():
            corpus.curations = []

    def get_corpus(self, corpus_id, check_s3=True, use_cache=True):
        """Return a corpus given an ID.

        If the corpus ID cannot be found, an InvalidCorpusError is raised.

        Parameters
        ----------
        corpus_id : str
            The ID of the corpus to return.
        check_s3 : bool
            If True, look on S3 for the corpus if it's not currently loaded.
            Default: True
        use_cache : bool
            If True, look in local cache before trying to find corpus on s3.
            If True while check_s3 if False, this option will be ignored.
            Default: False.

        Returns
        -------
        Corpus
            The corpus with the given ID.
        """
        logger.info('Getting corpus "%s"' % corpus_id)
        corpus = self.corpora.get(corpus_id)
        if corpus:
            logger.info('Found corpus loaded in memory')
        if check_s3 and corpus is None:
            logger.info('Corpus not loaded, looking on S3')
            corpus = Corpus.load_from_s3(corpus_id,
                                         force_s3_reload=not use_cache,
                                         raise_exc=True)
            corpus.cache = self.cache
            logger.info('Adding corpus to loaded corpora')
            self.corpora[corpus_id] = corpus

            # Run update beliefs. The belief update needs to be inside this
            # if statement to avoid infinite recursion
            beliefs = self.update_beliefs(corpus_id)
        elif corpus is None:
            raise InvalidCorpusError

        return corpus

    def get_curations(self, corpus_id, reader):
        """Download curations for corpus id filtered to reader

        Parameters
        ----------
        corpus_id: str
            The ID of the corpus to download curations from
        reader : str
            The name of the reader to filter to. Has to be among valid
            reader names of 'all'.

        Returns
        -------
        dict
            A dict containing the requested curations
        """
        logger.info('Getting curations for corpus %s' % corpus_id)
        corpus = self.get_corpus(corpus_id, check_s3=True, use_cache=True)
        corpus_curations = corpus.get_curations(look_in_cache=True)
        # Get all statements that have curations
        curated_stmts = {}
        for curation in corpus_curations:
            uuid = curation['statement_id']
            curated_stmts[uuid] = corpus.statements[uuid]
        if reader and reader != 'all':
            # Filter out statements and curations that don't contain material
            # from provided reader (in source api of statement)
            filtered_curations = {}
            filtered_stmts = {}
            for uuid, stmt in curated_stmts.items():
                # Check if any of the evidences are from the provided reader
                for ev in stmt.evidence:
                    if ev.source_api == reader.lower():
                        filtered_stmts[uuid] = stmt
                        filtered_curations[uuid] = corpus_curations[uuid]
                        break
            data = {'curations': filtered_curations,
                    'statements': {uuid: st.to_json() for uuid, st in
                                   filtered_stmts.items()}}
        else:
            data = {'curations': corpus_curations,
                    'statements': {uuid: st.to_json() for uuid, st in
                                   curated_stmts.items()}}
        return data

    def submit_curations(self, curations, save=True):
        """Submit correct/incorrect curations fo a given corpus.

        Parameters
        ----------
        curations : list of dict
            A list of curationss.
        save : bool
            If True, save the updated curations to the local cache.
            Default: True
        """
        logger.info('Submitting %d curations' % len(curations))
        for curation in curations:
            self.submit_curation(curation, save=save)

    def submit_curation(self, curation, save=True):
        try:
            corpus_id = curation['corpus_id']
            uuid = curation['statement_id']
            update_type = curation['update_type']
        except KeyError:
            raise ValueError('Required parameters missing.')
        belief_count_idx = 0 if update_type in correct_flags else 1
        # Try getting the corpus first
        corpus = self.get_corpus(corpus_id, check_s3=True, use_cache=True)
        # Start tabulating the curation counts
        prior_counts = {}
        subtype_counts = {}
        # Take each curation from the input
        stmt = corpus.statements.get(uuid)
        if stmt is None:
            logger.warning('%s is not in the corpus.' % uuid)
            return None
        # Save the curation in the corpus
        corpus.curations.append(curation)
        # Now take all the evidences of the statement and assume that
        # they follow the correctness of the curation and contribute to
        # counts for their sources
        for ev in stmt.evidence:
            # Make the index in the curation count list
            extraction_rule = ev.annotations.get('found_by')
            # If there is no extraction rule then we just score the source
            if not extraction_rule:
                try:
                    prior_counts[ev.source_api][belief_count_idx] += 1
                except KeyError:
                    prior_counts[ev.source_api] = [0, 0]
                    prior_counts[ev.source_api][belief_count_idx] += 1
            # Otherwise we score the specific extraction rule
            else:
                try:
                    subtype_counts[ev.source_api][extraction_rule][belief_count_idx] \
                        += 1
                except KeyError:
                    if ev.source_api not in subtype_counts:
                        subtype_counts[ev.source_api] = {}
                    subtype_counts[ev.source_api][extraction_rule] = [0, 0]
                    subtype_counts[ev.source_api][extraction_rule][belief_count_idx] \
                        += 1
        # Finally, we update the scorer with the new curation counts
        self.scorer.update_counts(prior_counts, subtype_counts)

        # Save the updated curations to S3 and cache
        if save:
            corpus.save_curations_to_cache()

    def save_curations(self, corpus_id, save_to_cache=True):
        """Save the current state of curations for a corpus given its ID

        If the corpus ID cannot be found, an InvalidCorpusError is raised.

        Parameters
        ----------
        corpus_id : str
            the ID of the corpus to save the
        save_to_cache : bool
            If True, also save the current curation to the local cache.
            Default: True.
        """
        # Do NOT use cache or S3 when getting the corpus, otherwise it will
        # overwrite the current corpus
        logger.info('Saving curations for corpus "%s"' % corpus_id)
        corpus = self.get_corpus(corpus_id, check_s3=False, use_cache=False)
        corpus.upload_curations(corpus_id, save_to_cache=save_to_cache)

    def update_metadata(self, corpus_id, meta_data, save_to_cache=True):
        """Update the meta data for a given corpus

        Parameters
        ----------
        corpus_id : str
            The ID of the corpus to update the meta data for
        meta_data : dict
            A json compatible dict containing the meta data
        save_to_cache : bool
            If True, also update the local cache of the meta data dict.
            Default: True.
        """
        logger.info('Updating meta data for corpus "%s"' % corpus_id)
        corpus = self.get_corpus(corpus_id, check_s3=True, use_cache=True)

        # Loop and add/overwrite meta data key value pairs
        for k, v in meta_data.items():
            corpus.meta_data[k] = v

        if save_to_cache:
            meta_file_key = '%s/%s/%s.json' % (default_key_base,
                                               corpus_id,
                                               file_defaults['meta'])
            corpus._save_to_cache(meta=meta_file_key)

    def update_beliefs(self, corpus_id):
        """Return updated belief scores for a given corpus.

        Parameters
        ----------
        corpus_id : str
            The ID of the corpus for which beliefs are to be updated.

        Returns
        -------
        dict
            A dictionary of belief scores with keys corresponding to Statement
            UUIDs and values to new belief scores.
        """
        logger.info('Updating beliefs for corpus "%s"' % corpus_id)
        # TODO check which options are appropriate for get_corpus
        corpus = self.get_corpus(corpus_id)
        be = BeliefEngine(self.scorer)
        stmts = list(corpus.statements.values())
        be.set_prior_probs(stmts)
        # Here we set beliefs based on actual curation
        for curation in corpus.curations:
            stmt = corpus.statements.get(curation['statement_id'])
            if stmt is None:
                logger.warning('%s is not in the corpus.' %
                               curation['statement_id'])
                continue
            # If the statement was thrown away, we set its belief to 0
            if curation['update_type'] == 'discard_statement':
                stmt.belief = 0
            # In this case the statement was either vetted to be correct
            # or was corrected manually
            else:
                stmt.belief = 1
        belief_dict = {st.uuid: st.belief for st in stmts}
        return belief_dict

    def run_assembly(self, corpus_id, project_id=None):
        from indra.preassembler import Preassembler

        corpus = self.get_corpus(corpus_id)

        # STAGE 1: remove any discarded statements
        discard_curations = self.get_project_curations(corpus_id, project_id,
                                                       'discard_statement')
        discard_stmt_raw_ids = \
            self.get_raw_stmt_ids_for_curations(corpus_id, discard_curations)

        raw_stmts = [s for s in corpus.raw_statements
                     if s.uuid not in discard_stmt_raw_ids]

        # STAGE 2: grounding
        grounding_curations = self.get_project_curations(corpus_id, project_id,
                                                         'factor_grounding')
        # Since this is an expensive step, we only do it if there are actual
        # grounding changes
        if grounding_curations:
            # Modify the ontology here according to any grounding
            # updates
            for cur in grounding_curations:
                txt, grounding = parse_factor_grounding_curation(cur)
                self.ont_manager.add_entry(grounding, [txt])

            # Send the latest ontology and list of concept texts to Eidos
            yaml_str = yaml.dump(self.ont_manager.yml)
            concepts = []
            for stmt in raw_stmts:
                for concept in stmt.agent_list():
                    concept_txt = concept.db_refs.get('TEXT')
                    concepts.append(concept_txt)
            groundings = reground_texts(concepts, yaml_str,
                                        webservice=self.eidos_url)
            # Update the corpus with new groundings
            idx = 0
            for stmt in raw_stmts:
                for concept in stmt.agent_list():
                    concept.db_refs['WM'] = groundings[idx]
                    idx += 1

        # STAGE 3: run normalization
        pa = Preassembler(ontology=self.ont_manager, stmts=raw_stmts)
        pa.normalize_equivalences('WM')
        pa.normalize_opposites('WM')
        stmts = pa.stmts

        # STAGE 4: apply polarity curations
        polarity_curations = self.get_project_curations(corpus_id, project_id,
                                                        'factor_polarity')
        for cur in polarity_curations:
            polarity_stmt_raw_ids = \
                self.get_raw_stmt_ids_for_curations(corpus_id, [cur])
            role, new_polarity = parse_factor_polarity_curation(cur)
            for stmt in stmts:
                if stmt.uuid in polarity_stmt_raw_ids:
                    if role == 'subj':
                        stmt.subj.delta.polarity = new_polarity
                    elif role == 'obj':
                        stmt.obj.delta.polarity = new_polarity

        # STAGE 5: apply reverse relation curations
        reverse_curations = self.get_project_curations(corpus_id, project_id,
                                                       'reverse_relation')
        reverse_stmt_raw_ids = \
            self.get_raw_stmt_ids_for_curations(corpus_id, reverse_curations)
        for stmt in stmts:
            if stmt.uuid in reverse_stmt_raw_ids:
                tmp = stmt.subj
                stmt.subj = stmt.obj
                stmt.obj = tmp
                # TODO: update any necessary annotations

        # STAGE 6: run preassembly
        stmts = ac.run_preassembly(stmts,
                                   belief_scorer=self.scorer,
                                   return_toplevel=False,
                                   poolsize=4,
                                   ontology=self.ont_manager)
        # TODO: do these need to be done before polarity curation?
        stmts = ac.merge_groundings(stmts)
        stmts = ac.merge_deltas(stmts)
        stmts = ac.standardize_names_groundings(stmts)

        # STAGE 7: persist results either as an S3 dump or by
        # rewriting the corpus
        stmt_dict = {s.uuid: s for s in stmts}
        if project_id:
            self.dump_project(corpus_id, project_id, stmt_dict)
        else:
            # TODO: shouldn't we do an S3 dump here?
            corpus.statements = stmt_dict
        return stmts

    def dump_project(self, corpus_id, project_id, stmts):
        import json
        from indra.statements import stmts_to_json
        from . import default_bucket, default_profile, default_key_base
        # Structure and upload assembled statements
        stmts_json = '\n'.join(json.dumps(jo) for jo in
                               stmts_to_json(list(stmts.values())))
        Corpus._s3_put_file(
            Corpus._make_s3_client(default_profile),
            f'{default_key_base}/{corpus_id}/{project_id}/statements.json',
            stmts_json, default_bucket)

    def get_project_curations(self, corpus_id, project_id,
                              curation_type=None):
        corpus = self.get_corpus(corpus_id)
        return [cur for cur in corpus.curations
                if cur['project_id'] == project_id
                and not curation_type or cur['update_type'] == curation_type]

    def get_raw_stmt_ids_for_curations(self, corpus_id, curations):
        corpus = self.get_corpus(corpus_id)
        stmt_raw_ids = set()
        for cur in curations:
            for ev in corpus.statements[cur['statement_id']].evidence:
                stmt_raw_ids |= set(ev.annotations['prior_uuids'])
        return stmt_raw_ids


def parse_factor_grounding_curation(cur):
    bef_subj = cur['before']['subj']
    bef_obj = cur['before']['obj']
    aft_subj = cur['after']['subj']
    aft_obj = cur['after']['obj']

    if bef_subj['concept'] != aft_subj['concept']:
        return aft_subj['factor'], aft_subj['concept']
    elif bef_obj['concept'] != aft_obj['concept']:
        return aft_obj['factor'], aft_obj['concept']
    else:
        return None, None


def parse_factor_polarity_curation(cur):
    bef_subj = cur['before']['subj']
    bef_obj = cur['before']['obj']
    aft_subj = cur['after']['subj']
    aft_obj = cur['after']['obj']

    if bef_subj['polarity'] != aft_subj['polarity']:
        return 'subj', aft_subj['polarity']
    elif bef_obj['polarity'] != aft_obj['polarity']:
        return 'obj', aft_obj['polarity']
    else:
        return None, None


correct_flags = {'vet_statement'}
incorrect_flags = {'factor_grounding', 'discard_statement', 'reverse_relation',
                   'factor_polarity'}
