class IncrementalAssembler:
    def __init__(self, prepared_stmts):
        self.prepared_stmts = prepared_stmts
        self.stmts_by_hash = {}
        self.evs_by_stmt_hash = {}
        for stmt in prepared_stmts:
            stmt, evs = separate_evidence(stmt)
            stmt_hash = stmt.get_hash()
            self.stmts_by_hash[stmt_hash] = stmt
            self.evs_by_stmt_hash[stmt_hash] = evs


def separate_evidence(stmt):
    evs = stmt.evidence
    stmt.evidence = []
    return stmt, evs