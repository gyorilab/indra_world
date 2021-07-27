import csv
from indra.statements import *
stmts = stmts_from_json_file('august_embed_10k.json', format='jsonl')
concepts = []
for stmt in stmts:
    for concept in stmt.agent_list():
        concepts.append(concept)
problems = []
for concept in concepts:
    if 'WM' in concept.db_refs:
        gr = concept.db_refs['WM'][0]
        if gr[0] and gr[2]:
            if 'wm/process' in gr[0][0] and 'wm/concept' in gr[2][0]:
                problems.append(concept)
rows = [[problem.db_refs['TEXT'], problem.db_refs['WM'][0][0][0],
         problem.db_refs['WM'][0][2][0]] for problem in problems
        if not problem.db_refs['WM'][0][1] and not problem.db_refs['WM'][0][3]]
rows = sorted(rows, key=lambda x: x[0])
rows = [['text', 'theme', 'process']] + rows
with open('grounding_concept_of_process.csv', 'w') as fh:
    wr = csv.writer(fh, delimiter=',')
    wr.writerows(rows)

