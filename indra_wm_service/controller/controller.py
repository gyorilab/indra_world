import datetime
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

    def new_project(self, project_id, name):
        self.db.add_project(project_id, name)

    def load_project(self, project_id, record_keys=None):
        # 1. Select records associated with project
        if record_keys is None:
            record_keys = self.db.get_records_for_project(project_id)
        # 2. Select statements from prepared stmts table
        prepared_stmts = []
        for record_key in record_keys:
            prepared_stmts += self.db.get_statements_for_record(record_key)
        # 3. Initiate an assembler
        assembler = IncrementalAssembler(prepared_stmts)
        self.assemblers[project_id] = assembler

    def unload_project(self, project_id):
        self.assemblers.pop(project_id, None)

    def add_dart_record(self, reader, reader_version, document_id,
                        storage_key, date=None):
        if date is None:
            date = datetime.datetime.utcnow().isoformat()
        self.db.add_dart_record(reader, reader_version, document_id,
                                storage_key, date)

    def process_dart_record(self, reader, reader_version, document_id,
                            storage_key, local_storage=None,
                            grounding_mode='compositional',
                            extract_filter='influence'):
        record = \
            {'identity': reader,
             'version': reader_version,
             'document_id': document_id,
             'storage_key': storage_key}
        reader_outputs = download_records([record], local_storage=local_storage)
        content = reader_outputs[reader][document_id]
        return self.add_reader_output(content, record,
                                      grounding_mode=grounding_mode,
                                      extract_filter=extract_filter)

    def add_reader_output(self, content, record,
                          grounding_mode='compositional',
                          extract_filter='influence'):
        stmts = process_reader_output(content, record,
                                      grounding_mode=grounding_mode,
                                      extract_filter=extract_filter)
        return self.add_reader_statements(stmts, record)

    def add_reader_statements(self, stmts, record):
        prepared_stmts = preparation_pipeline.run(stmts)
        return self.add_prepared_statements(prepared_stmts, record)

    def add_prepared_statements(self, prepared_stmts, record):
        self.db.add_statements_for_record(record_key=record['storage_key'],
                                          # FIXME: how should we set the
                                          # version here?
                                          indra_version='1.0',
                                          stmts=prepared_stmts)

    def assemble_new_records(self, project_id, new_record_keys):
        # 1. We get all the records associated with the project
        # which may or may not include some of the new ones
        record_keys = self.db.get_records_for_project(project_id)
        old_record_keys = list(set(record_keys) - set(new_record_keys))
        # 2. Now load the project with the old record keys
        self.load_project(project_id, old_record_keys)
        # 3. Now get the new statements associated with the new records
        new_stmts = []
        for record_key in new_record_keys:
            stmts = self.db.get_statements_for_record(record_key)
            new_stmts += stmts
        # 4. Finally get an incremental assembly delta and return it
        delta = self.assemblers[project_id].add_statements(new_stmts)
        return delta

    def add_curation(self, project_id, curation):
        self.db.add_curation_for_project(project_id, curation)

    def add_project_records(self, project_id, record_keys):
        self.db.add_records_for_project(project_id, record_keys)