"""This script generates a dump of cases where both A-B and B-A appear
as a theme-property/process pair."""

import csv
from collections import defaultdict
from indra.statements import *

if __name__ == '__main__':
    concepts = []
    stmts = stmts_from_json_file('august_embed_10k.json', format='jsonl')
    for stmt in stmts:
        for concept in stmt.agent_list():
            concepts.append(concept)
    groundings = defaultdict(list)
    for concept in concepts:
        if 'WM' in concept.db_refs:
            gr = concept.db_refs['WM'][0]
            if gr[0] and gr[1] and not gr[2] and not gr[3]:
                groundings[(gr[0][0].split('/')[-1],
                            gr[1][0].split('/')[-1], 'property')].\
                    append(concept.db_refs['TEXT'])
            elif gr[0] and gr[2] and not gr[1] and not gr[3]:
                groundings[(gr[0][0].split('/')[-1],
                            gr[2][0].split('/')[-1], 'process')].\
                    append(concept.db_refs['TEXT'])
    pairs = {'property': set(), 'process': set()}
    for theme, pr, type in groundings.keys():
        if (pr, theme, type) in groundings:
            pairs[type].add(tuple(sorted([pr, theme])))

    rows = [['type', 'first element', 'second element', 'example theme=first',
             'example theme=second']]
    for type, pair_set in pairs.items():
        for gr1, gr2 in pair_set:
            row = [type, gr1, gr2, groundings[(gr1, gr2, type)][0],
                   groundings[(gr2, gr1, type)][0]]
            rows.append(row)

    with open('theme_process_property_and_reverse.csv', 'w') as fh:
        writer = csv.writer(fh, delimiter=',')
        writer.writerows(rows)