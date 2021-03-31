from copy import deepcopy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from sqlalchemy import and_, insert, create_engine
from indra.statements import stmts_from_json, stmts_to_json
import indra_wm_service.db.schema as wms_schema


class DbManager:
    """Manages transactions with the assembly database and exposes an API
    for various operations."""
    def __init__(self, url):
        self.url = make_url(url)
        self.engine = create_engine(self.url)
        self.session = None

    def get_session(self):
        """Return the current active session or create one if not available."""
        if self.session is None:
            session_maker = sessionmaker(bind=self.engine)
            self.session = session_maker()
        return self.session

    def create_all(self):
        """Create all the database tables in the schema."""
        wms_schema.Base.metadata.create_all(self.engine)

    def query(self, *query_args):
        """Run and return results of a generic query."""
        session = self.get_session()
        return session.query(*query_args)

    def sql_query(self, query_str):
        """Run and return results of a generic SQL query."""
        return self.engine.execute(query_str)

    def execute(self, operation):
        """Execute an operation on the current session and return results."""
        session = self.get_session()
        return session.execute(operation)

    def add_project(self, project_id, name):
        """Add a new project.

        Parameters
        ----------
        project_id : int
            The project ID.
        name : str
            The project name
        """
        op = insert(wms_schema.Projects).values(id=project_id,
                                                name=name)
        return self.execute(op)

    def add_documents_for_project(self, project_id, doc_ids):
        """Add document IDs for a project with the given ID."""
        op = insert(wms_schema.ProjectDocuments).values(
            [
                {'project_id': project_id,
                 'document_id': doc_id}
                for doc_id in doc_ids
            ]
        )
        return self.execute(op)

    def get_documents_for_project(self, project_id):
        qfilter = wms_schema.ProjectDocuments.project_id.like(project_id)
        q = self.query(wms_schema.ProjectDocuments.document_id).filter(qfilter)
        doc_ids = [r[0] for r in q.all()]
        return doc_ids

    def add_corpus(self, corpus_id, metadata):
        op = insert(wms_schema.Corpora).values(id=corpus_id,
                                               meta_data=metadata)
        return self.execute(op)

    def add_documents_for_corpus(self, corpus_id, doc_ids):
        op = insert(wms_schema.CorpusDocuments).values(
            [
                {'corpus_id': corpus_id,
                 'document_id': doc_id}
                for doc_id in doc_ids
            ]
        )
        return self.execute(op)

    def get_documents_for_corpus(self, corpus_id):
        qfilter = wms_schema.CorpusDocuments.corpus_id.like(corpus_id)
        q = self.query(wms_schema.CorpusDocuments.document_id).filter(qfilter)
        doc_ids = [r[0] for r in q.all()]
        return doc_ids

    def add_statements_for_document(self, document_id, reader, reader_version,
                                    indra_version, stmts):
        """Add a set of prepared statements for a given document."""
        op = insert(wms_schema.PreparedStatements).values(
            [
                {
                    'document_id': document_id,
                    'reader': reader,
                    'reader_version': reader_version,
                    'indra_version': indra_version,
                    'stmt': stmt
                 }
                # Note: the deepcopy here is done because when dumping
                # statements into JSON, the hash is overwritten, potentially
                # with an inadequate one (due to a custom matches_fun not being
                # given here).
                for stmt in stmts_to_json(deepcopy(stmts))
            ]
        )
        return self.execute(op)

    def add_curation_for_project(self, project_id, curation):
        """Add curations for a given project."""
        op = insert(wms_schema.Curations).values(project_id=project_id,
                                                 curation=curation)
        return self.execute(op)

    def get_statements_for_document(self, document_id, reader=None,
                                    reader_version=None, indra_version=None):
        """Return prepared statements for a given document."""
        qfilter = wms_schema.PreparedStatements.document_id.like(document_id)
        if reader:
            qfilter = and_(
                qfilter,
                wms_schema.PreparedStatements.reader.like(reader)
            )
        if reader_version:
            qfilter = and_(
                qfilter,
                wms_schema.PreparedStatements.reader_version.like(reader_version)
            )
        if indra_version:
            qfilter = and_(
                qfilter,
                wms_schema.PreparedStatements.indra_version.like(indra_version)
            )

        q = self.query(wms_schema.PreparedStatements.stmt).filter(qfilter)
        stmts = stmts_from_json([r[0] for r in q.all()])
        return stmts

    def get_curations_for_project(self, project_id):
        """Return curations for a given project"""
        qfilter = wms_schema.Curations.project_id.like(project_id)
        q = self.query(wms_schema.Curations.curation).filter(qfilter)
        curations = [res[0] for res in q.all()]
        return curations

    def add_dart_record(self, reader, reader_version, document_id, storage_key,
                        date):
        op = insert(wms_schema.DartRecords).values(
                **{
                    'reader': reader,
                    'reader_version': reader_version,
                    'document_id': document_id,
                    'storage_key': storage_key,
                    'date': date
                }
        )
        return self.execute(op)

    def get_dart_record(self, reader, document_id, reader_version=None):
        qfilter = wms_schema.DartRecords.document_id.like(document_id)
        qfilter = and_(qfilter, wms_schema.DartRecords.reader.like(reader))
        if reader_version:
            qfilter = and_(qfilter, wms_schema.DartRecords.
                           reader_version.like(reader_version))
        q = self.query(wms_schema.DartRecords.storage_key).filter(qfilter)
        keys = [r[0] for r in q.all()]
        return keys
