from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, JSON

Base = declarative_base()


class Projects(Base):
    __tablename__ = 'projects'
    id = Column(String, primary_key=True)
    name = Column(String)
    ontology_id = Column(Integer)


class ProjectDocuments(Base):
    __tablename__ = 'project_documents'
    _dummy = Column(Integer, primary_key=True)
    project_id = Column(String)
    document_id = Column(String)


class PreparedStatements(Base):
    __tablename__ = 'prepared_statements'
    _dummy = Column(Integer, primary_key=True)
    document_id = Column(String)
    reader = Column(String)
    reader_version = Column(String)
    indra_version = Column(String)
    stmt = Column(JSON)


class Curations(Base):
    __tablename__ = 'curations'
    _dummy = Column(Integer, primary_key=True)
    project_id = Column(String)
    curation = Column(JSON)


class Ontologies(Base):
    __tablename__ = 'ontologies'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    ontology = Column(String)


class DartRecords(Base):
    __tablename__ = 'dart_records'
    _dummy = Column(Integer, primary_key=True)
    document_id = Column(String)
    storage_key = Column(String)
    reader_version = Column(String)
    reader = Column(String)
    date = Column(String)


class Corpora(Base):
    __tablename__ = 'corpora'
    _dummy = Column(Integer, primary_key=True)
    id = Column(String)
    meta_data = Column(JSON)


class CorpusDocuments(Base):
    __tablename__ = 'corpus_documents'
    _dummy = Column(String, primary_key=True)
    corpus_id = Column(String)
    document_id = Column(String)