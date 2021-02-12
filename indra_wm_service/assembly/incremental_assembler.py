from copy import deepcopy


class IncrementalAssembler:
    def __init__(self, prepared_stmts, refinement_filters):
        self.prepared_stmts = prepared_stmts
        # FIXME: This should be set once the assembly filter
        # classes are implemented
        self.refinement_filters = refinement_filters
        # These are preassembly data structures
        self.stmts_by_hash = {}
        self.evs_by_stmt_hash = {}
        self.refinement_edges = set()

    def deduplicate(self):
        for stmt in self.prepared_stmts:
            stmt_hash = stmt.get_hash()
            evs = stmt.evidence
            if stmt_hash not in self.stmts_by_hash:
                stmt.evidence = []
                self.stmts_by_hash = stmt
            self.evs_by_stmt_hash[stmt_hash] += evs

    def get_refinements(self):
        return []