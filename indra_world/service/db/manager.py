import logging
from copy import deepcopy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from sqlalchemy import and_, insert, create_engine
from indra.statements import stmts_from_json, stmts_to_json
from . import schema as wms_schema

logger = logging.getLogger(__name__)


class DbManager:
    """Manages transactions with the assembly database and exposes an API
    for various operations."""
    def __init__(self, url):
        self.url = make_url(url)
        logger.info('Starting DB manager with URL: %s' % str(self.url))
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
        try:
            res = session.execute(operation)
            session.commit()
            return {'rowcount': res.rowcount,
                    'inserted_primary_key': res.inserted_primary_key}
        except SQLAlchemyError as e:
            logger.error(e)
            session.rollback()
            return None

    def add_project(self, project_id, name):
        """Add a new project.

        Parameters
        ----------
        project_id : str
            The project ID.
        name : str
            The project name
        """
        op = insert(wms_schema.Projects).values(id=project_id,
                                                name=name)
        return self.execute(op)

    def add_records_for_project(self, project_id, record_keys):
        """Add document IDs for a project with the given ID."""
        op = insert(wms_schema.ProjectRecords).values(
            [
                {'project_id': project_id,
                 'record_key': rec_key}
                for rec_key in record_keys
            ]
        )
        return self.execute(op)

    def get_records_for_project(self, project_id):
        qfilter = and_(wms_schema.ProjectRecords.project_id.like(project_id))
        q = self.query(wms_schema.ProjectRecords.record_key).filter(qfilter)
        record_keys = [r[0] for r in q.all()]
        return record_keys

    def get_documents_for_project(self, project_id):
        qfilter = and_(
            wms_schema.ProjectRecords.project_id.like(project_id),
            (wms_schema.DartRecords.storage_key ==
             wms_schema.ProjectRecords.record_key))
        q = self.query(wms_schema.DartRecords.document_id).filter(qfilter)
        doc_ids = sorted(set(r[0] for r in q.all()))
        return doc_ids

    def get_projects(self):
        q = self.query(wms_schema.Projects)
        projects = [{'id': p.id, 'name': p.name} for p in q.all()]
        return projects

    def add_corpus(self, corpus_id, metadata):
        op = insert(wms_schema.Corpora).values(id=corpus_id,
                                               meta_data=metadata)
        return self.execute(op)

    def add_records_for_corpus(self, corpus_id, record_keys):
        op = insert(wms_schema.CorpusRecords).values(
            [
                {'corpus_id': corpus_id,
                 'record_key': rec_key}
                for rec_key in record_keys
            ]
        )
        return self.execute(op)

    def get_records_for_corpus(self, corpus_id):
        qfilter = and_(wms_schema.CorpusRecords.corpus_id.like(corpus_id))
        q = self.query(wms_schema.CorpusRecords.record_key).filter(qfilter)
        record_keys = [r[0] for r in q.all()]
        return record_keys

    def get_documents_for_corpus(self, corpus_id):
        qfilter = and_(
            wms_schema.CorpusRecords.corpus_id.like(corpus_id),
            (wms_schema.DartRecords.storage_key ==
             wms_schema.CorpusRecords.record_key))
        q = self.query(wms_schema.DartRecords.document_id).filter(qfilter)
        doc_ids = sorted(set(r[0] for r in q.all()))
        return doc_ids

    def add_statements_for_record(self, record_key, stmts, indra_version):
        """Add a set of prepared statements for a given document."""
        if not stmts:
            return None
        op = insert(wms_schema.PreparedStatements).values(
            [
                {
                    'record_key': record_key,
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

    def add_curation_for_project(self, project_id, stmt_hash, curation):
        """Add curations for a given project."""
        op = insert(wms_schema.Curations).values(project_id=project_id,
                                                 stmt_hash=stmt_hash,
                                                 curation=curation)
        return self.execute(op)

    def get_statements_for_record(self, record_key):
        """Return prepared statements for given record."""
        qfilter = wms_schema.PreparedStatements.record_key.like(record_key)
        q = self.query(wms_schema.PreparedStatements.stmt).filter(qfilter)
        stmts = stmts_from_json([r[0] for r in q.all()])
        return stmts

    def get_statements(self):
        """Return all prepared statements in the DB."""
        q = self.query(wms_schema.PreparedStatements.stmt)
        stmts = stmts_from_json([r[0] for r in q.all()])
        return stmts

    def get_statements_for_document(self, document_id, reader=None,
                                    reader_version=None, indra_version=None):
        """Return prepared statements for a given document."""
        qfilter = and_(
            wms_schema.DartRecords.document_id.like(document_id),
            wms_schema.DartRecords.storage_key ==
            wms_schema.PreparedStatements.record_key)
        if reader:
            qfilter = and_(
                qfilter,
                wms_schema.DartRecords.reader.like(reader)
            )
        if reader_version:
            qfilter = and_(
                qfilter,
                wms_schema.DartRecords.reader_version.like(reader_version)
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
        q = self.query(wms_schema.Curations).filter(qfilter)
        # Build a dict of stmt_hash: curation records
        curations = {res.stmt_hash: res.curation for res in q.all()}
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

    def get_dart_records(self, reader, document_id, reader_version=None):
        # TODO: allow more optional parameters
        qfilter = wms_schema.DartRecords.document_id.like(document_id)
        qfilter = and_(qfilter, wms_schema.DartRecords.reader.like(reader))
        if reader_version:
            qfilter = and_(qfilter, wms_schema.DartRecords.
                           reader_version.like(reader_version))
        q = self.query(wms_schema.DartRecords.storage_key).filter(qfilter)
        keys = [r[0] for r in q.all()]
        # TODO: should we just return the keys here or the full record?
        # maybe add a different function for getting keys
        return keys
