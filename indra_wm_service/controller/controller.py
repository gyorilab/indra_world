from indra_wm_service.db.manager import DbManager
from indra_wm_service.assembly.incremental_assembler import \
    IncrementalAssembler


class ServiceController:
    def __init__(self, db_url):
        self.db = DbManager(db_url)
        self.assemblers = {}

    def load_project(self, project_id, name, doc_ids):
        # 1. Add project to DB
        self.db.add_project(project_id, name)
        # 2. Add project documents to table
        self.db.add_documents_for_project(project_id,
                                          doc_ids)
        # 3. FIXME: we may need to process reader output
        # from DART and dump pre-processed statements in
        # the DB first.

        # 4. Select statements from prepared stmts table
        # TODO: implement a get_statements_for_project function in the
        # DbManager and use it here
        prepared_stmts = []
        for doc_id in doc_ids:
            prepared_stmts += self.db.get_statements_for_document(doc_id)

        # 5. Initiate an assembler
        assembler = IncrementalAssembler(prepared_stmts)
        self.assemblers[project_id] = assembler

    def remove_project(self, project_id):
        self.assemblers.pop(project_id, None)