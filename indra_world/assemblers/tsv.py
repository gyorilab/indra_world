"""Assemble tab separated spreadsheets of assembled INDRA Statements for
curation purposes."""
import csv
import tqdm
from collections import defaultdict
from indra.statements import Migration


grounding_mode = 'flat'


class TsvAssembler:
    def __init__(self, statements):
        self.statements = statements

    def make_model(self, fname):
        stmts_by_grounding = {'subj': defaultdict(list),
                              'obj': defaultdict(list)}
        for stmt in self.statements:
            for role, concept in [('subj', stmt.subj.concept),
                                  ('obj', stmt.obj.concept)]:
                if grounding_mode == 'compositional':
                    grounding = concept.db_refs['WM_FLAT'][0]['grounding']
                else:
                    grounding = concept.db_refs['WM'][0][0]
                stmts_by_grounding[role][grounding].append(stmt)

        rows = []
        for role, groundings in stmts_by_grounding.items():
            for grounding, statements in tqdm.tqdm(sorted(groundings.items(),
                                                          key=lambda x: x[0])):
                for stmt in statements:
                    rows += self.make_rows(stmt, role, grounding)
        header = ['role', 'grounding', 'grounding_suffix',
                  'entity_text', 'score',
                  'stmt_id', 'belief',
                  'location', 'time', 'polarity', 'adjectives',
                  'reader', 'sentence', 'full_raw_grounding']
        with open(fname, 'w') as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)

    @staticmethod
    def make_rows(stmt, role, grounding):
        return [TsvAssembler.make_row(stmt, ev, role, grounding)
                for ev in stmt.evidence]

    @staticmethod
    def make_row(stmt, evidence, role, grounding):
        agent_idx = 0 if role == 'subj' else 1
        grounding_suffix = grounding.split('/')[-1]
        event = getattr(stmt, role)
        if isinstance(event, Migration):
            polarity = ''
            adjectives = ''
            location = ''
            time = ''
        else:
            polarity = event.delta.polarity
            adjectives = '|'.join(event.delta.adjectives) \
                if event.delta.adjectives else ''
            location = str(event.context.geo_location) \
                if event.context and event.context.geo_location else ''
            time = str(event.context.time) \
                if event.context and event.context.time else ''
        concept = event.concept
        if grounding_mode == 'compositional':
            grounding_entry = concept.db_refs['WM_FLAT'][0]
            score = grounding_entry['score']
        else:
            grounding_entry = concept.db_refs['WM'][0]
            score = grounding_entry[1]
        annot_grounding = \
            evidence.annotations['agents']['raw_grounding'][agent_idx]
        full_raw_grounding = str(annot_grounding['WM'][0])
        reader = evidence.source_api
        text = evidence.text
        raw_text = evidence.annotations['agents']['raw_text'][agent_idx]
        row = [role, grounding, grounding_suffix, raw_text, score,
               stmt.uuid, stmt.belief,
               location, time, polarity, adjectives,
               reader, text, full_raw_grounding]
        return row


class TsvAssemblerStatement(TsvAssembler):
    def make_model(self, fname):
        stmts_by_grounding = defaultdict(list)
        for stmt in self.statements:
            groundings = []
            for concept in stmt.agent_list():
                if grounding_mode == 'compositional':
                    grounding = concept.db_refs['WM_FLAT'][0]['grounding']
                else:
                    grounding = concept.db_refs['WM'][0][0]
                groundings.append(grounding)
            stmts_by_grounding[tuple(groundings)].append(stmt)

        rows = []
        for groundings, statements in \
                tqdm.tqdm(sorted(stmts_by_grounding.items(),
                                 key=lambda x: x[0])):
            for stmt in statements:
                rows += self.make_rows(stmt, groundings)
        header = [
            'subj_grounding', 'subj_grounding_suffix',
            'subj_entity_text', 'subj_score',
            'subj_full_raw_grounding',
            'subj_location', 'subj_time', 'subj_polarity',
            'subj_adjectives',
            'obj_grounding', 'obj_grounding_suffix',
            'obj_entity_text', 'obj_score',
            'obj_full_raw_grounding',
            'obj_location', 'obj_time', 'obj_polarity',
            'obj_adjectives',
            'stmt_id', 'belief', 'reader', 'sentence',
            ]
        with open(fname, 'w') as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)

    @staticmethod
    def make_rows(stmt, groundings):
        return [TsvAssemblerStatement.make_row(stmt, ev, groundings)
                for ev in stmt.evidence]

    @staticmethod
    def make_row(stmt, evidence, groundings):
        row = []
        for agent_idx in [0, 1]:
            role = 'subj' if agent_idx == 0 else 'obj'
            grounding = groundings[agent_idx]
            grounding_suffix = grounding.split('/')[-1]
            event = getattr(stmt, role)
            if isinstance(event, Migration):
                polarity = ''
                adjectives = ''
                location = ''
                time = ''
            else:
                polarity = event.delta.polarity
                adjectives = '|'.join(event.delta.adjectives) \
                    if event.delta.adjectives else ''
                location = str(event.context.geo_location) \
                    if event.context and event.context.geo_location else ''
                time = str(event.context.time) \
                    if event.context and event.context.time else ''
            concept = event.concept
            if grounding_mode == 'compositional':
                grounding_entry = concept.db_refs['WM_FLAT'][0]
                score = grounding_entry['score']
            else:
                grounding_entry = concept.db_refs['WM'][0]
                score = grounding_entry[1]
            annot_grounding = \
                evidence.annotations['agents']['raw_grounding'][agent_idx]
            full_raw_grounding = str(annot_grounding['WM'][0])
            reader = evidence.source_api
            text = evidence.text
            raw_text = evidence.annotations['agents']['raw_text'][agent_idx]
            row += [grounding, grounding_suffix, raw_text, score,
                    full_raw_grounding,
                    location, time, polarity, adjectives]
        row += [stmt.uuid, stmt.belief, reader, text]
        return row
