"""This script creates a spreadhseet to evaluate which string
flattening approach works better."""
import csv
from indra.statements import *
from indra_world.assembly.operations import make_display_name, \
    make_display_name_linear


if __name__ == '__main__':
    stmts = stmts_from_json_file('august_embed_10k.json', format='jsonl')
    concepts = []
    for stmt in stmts:
        for concept in stmt.agent_list():
            concepts.append(concept)
    rows = []
    for concept in concepts:
        if 'WM' in concept.db_refs:
            gr = concept.db_refs['WM'][0]
            if gr[0] and not gr[1] and not gr[2] and not gr[3]:
                continue
            n1 = make_display_name(gr)
            n2 = make_display_name_linear(gr)
            labels = ['theme']
            if gr[1]:
                labels.append('property')
            if gr[2]:
                labels.append('process')
            if gr[3]:
                labels.append('process_property')
            rows.append([n1, n2, ','.join(labels)])
    rows = sorted(set([tuple(row) for row in rows]))
    with open('all_string_flattening.csv', 'w') as fh:
        writer = csv.writer(fh, delimiter=',')
        writer.writerows(rows)
