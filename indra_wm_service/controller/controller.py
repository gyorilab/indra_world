import itertools
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


expected_readers = {'eidos', 'hume', 'sofia'}


class ServiceController:
    def __init__(self, db_url):
        self.db = DbManager(db_url)
        self.assemblers = {}
        self.assembly_triggers = {}

    def new_project(self, project_id, name, doc_ids):
        # 1. Add project to DB
        self.db.add_project(project_id, name)
        # 2. Add project documents to table
        self.db.add_documents_for_project(project_id,
                                          doc_ids)

    def load_project(self, project_id):
        # 1. Select documents associated with project
        doc_ids = self.db.get_documents_for_project(project_id)
        # 2. Select statements from prepared stmts table
        prepared_stmts = []
        for doc_id in doc_ids:
            prepared_stmts += self.db.get_statements_for_document(doc_id)

        # 3. Initiate an assembler
        assembler = IncrementalAssembler(prepared_stmts)
        self.assemblers[project_id] = assembler

    def unload_project(self, project_id):
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
        return self.check_assembly_triggers_for_output(document_id, reader)

    def add_curation(self, project_id, curation):
        self.db.add_curation_for_project(project_id, curation)

    def add_project_documents(self, project_id, doc_ids):
        self.db.add_documents_for_project(project_id, doc_ids)
        # TODO: we need to check here if there are already prepared
        # statements for these documents. If there are then we can
        # run incremental assembly and return an assembly delta.
        self.assembly_triggers[project_id] = \
            {(reader, doc_id) for reader, doc_id
             in itertools.product(expected_readers, doc_ids)}
        return self.check_assembly_triggers_for_project(project_id)

    def add_dart_record(self, reader, reader_version, document_id, date):
        self.db.add_dart_record(reader, reader_version, document_id, date)

    def check_assembly_triggers_for_project(self, project_id=None):
        # Find trigger for project ID if any
        trigger = self.assembly_triggers.get(project_id)
        if not trigger:
            return None
        # Keep track of documents we need to add
        docs_with_records = []
        # Now check if we have records for each reader / document pair
        for reader, doc_id in trigger:
            rec = self.db.get_dart_record(reader, doc_id)
            # If no recotd, we return without doing anything else
            if not rec:
                return None
            docs_with_records.append(doc_id)
        # If we got this far, then all the requirements for the trigger
        # ere met so we can get all statements and add them to the project
        # to generate an assembly delta.
        all_stmts = []
        for doc_id in docs_with_records:
            stmts = self.db.get_statements_for_document(doc_id)
            all_stmts += stmts
        delta = self.assemblers[project_id].add_statements(all_stmts)
        return delta

    def check_assembly_triggers_for_output(self, document_id, reader):
        deltas = {}
        for project_id, conditions in self.assembly_triggers.items():
            if (document_id, reader) in conditions:
                delta = self.check_assembly_triggers_for_project(project_id)
                if delta:
                    deltas[project_id] = delta
        return deltas
