from sqlalchemy import insert
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
import indra_wm_service.db.schema as wms_schema


class DbManager:
    def __init__(self, url):
        self.url = make_url(url)
        self.engine = create_engine(self.url)
        self.session = None

    def get_session(self):
        if self.session is None:
            session_maker = sessionmaker(bind=self.engine)
            self.session = session_maker()
        return self.session

    def create_all(self):
        wms_schema.Base.metadata.create_all(self.engine)

    def query(self, *query_args):
        session = self.get_session()
        return session.query(*query_args)

    def sql_query(self, query_str):
        return self.engine.execute(query_str)

    def execute(self, operation):
        session = self.get_session()
        return session.execute(operation)

    def add_project(self, project_id, name):
        op = insert(wms_schema.Projects).values(id=project_id,
                                                name=name)
        return self.execute(op)

    def add_documents_for_project(self, project_id, doc_ids):
        op = insert(wms_schema.ProjectDocuments).values(
            [
                {'project_id': project_id,
                 'document_id': doc_id}
                for doc_id in doc_ids
            ]
        )
        return self.execute(op)

    def add_statements_for_document(self, document_id, reader_version,
                                    indra_version, stmts):
        op = insert(wms_schema.PreparedStatements).values(
            [
                {
                    'document_id': document_id,
                    'reader_version': reader_version,
                    'indra_version': indra_version,
                    'stmt': stmt
                 }
                for stmt in stmts
            ]
        )
        return self.execute(op)

    def add_curation_for_project(self, project_id, curation):
        op = insert(wms_schema.Curations).values(project_id=project_id,
                                                 curation=curation)
        return self.execute(op)

    def get_statements_for_document(self, document_id, reader_version=None,
                                    indra_version=None):
        qfilter = wms_schema.PreparedStatements.document_id.like(document_id)
        if reader_version:
            qfilter = qfilter.and_(
                wms_schema.PreparedStatements.reader_version.like(reader_version)
            )
        if indra_version:
            qfilter = qfilter.and_(
                wms_schema.PreparedStatements.indra_version.like(indra_version)
            )

        sess = self.get_session()
        q = sess.query(wms_schema.PreparedStatements.stmt).filter(qfilter)
        stmts = q.all()
        return stmts

    def get_curations_for_project(self, project_id):
        qfilter = wms_schema.Curations.project_id.like(project_id)
        sess = self.get_session()
        q = sess.query(wms_schema.Curations.curation).filter(qfilter)
        curations = q.all()
        return curations
