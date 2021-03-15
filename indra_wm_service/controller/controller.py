from indra.literature.dart_client import download_records
from indra_wm_service.db.manager import DbManager
from indra_wm_service.sources.dart import process_reader_output
from indra_wm_service.assembly.incremental_assembler import \
    IncrementalAssembler
from indra_wm_service.resources import get_resource_file
from indra.pipeline import AssemblyPipeline
from indra_wm_service.assembly.operations import *


preparation_pipeline = AssemblyPipeline.from_json_file(
    get_resource_file('statement_preparation.json'))


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

    def get_reader_output(self, record):
        # TODO: should we unpack this here and return just the output?
        return download_records([record])

    def add_reader_output(self, content, reader, reader_version, document_id):
        stmts = process_reader_output(reader, content, document_id, content)
        prepared_stmts = preparation_pipeline.run(stmts)
        self.db.add_statements_for_document(document_id,
                                            reader_version=reader_version,
                                            indra_version=1.0,
                                            stmts=prepared_stmts)
        # We need to check here if these statements need to be incrementally
        # assembled into any projects. Do we do that every time or only when
        # all the readings have become available?

    def add_curation(self, project_id, curation):
        self.db.add_curation_for_project(project_id, curation)

    def add_project_documents(self, project_id, doc_ids):
        self.db.add_documents_for_project(project_id, doc_ids)
        # TODO: we need to check here if there are already prepared
        # statements for these documents. If there are then we can
        # run incremental assembly and return an assembly delta.

    def add_dart_record(self, reader, reader_version, document_id, date):
        self.db.add_dart_record(reader, reader_version, document_id, date)