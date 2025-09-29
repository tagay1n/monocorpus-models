import os

from google.oauth2.service_account import Credentials
from sqlalchemy import Column, Integer, DateTime, String, Boolean
from sqlalchemy import select, insert, update
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session as _Session
from sqlalchemy.sql import func
import threading
import json

# The OAuth 2.0 scopes we need.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def get_credentials(token_file='token.json'):
    if not os.path.exists(token_file):
        raise ValueError(f"Token file '{token_file}' does not exist. Please authenticate first.")
    return Credentials.from_service_account_file(token_file)

class Base(DeclarativeBase):
    pass

class Document(Base):
    """
    Represents a document record with metadata and storage information.

    Attributes:
        md5 (str): Unique MD5 hash of the document, used as the primary key.
        mime_type (str): MIME type of the document (e.g., 'application/pdf').
        ya_path (str): Path to the document file in the storage system. Can be obsolete.
        ya_public_url (str): Public URL to the document on Yandex Disk.
        ya_public_key (str): Public key for accessing the document on Yandex Disk.
        ya_resource_id (str): Resource identifier on Yandex Disk.
        publisher (str): Name of the document's publisher.
        author (str): Name(s) of the document's author(s).
        title (str): Title of the document.
        isbn (str): International Standard Book Number.
        publish_date (str): Date(mostly just year) when the document was published.
        language (str): Language in which the document in format BCP-47
        genre (str): Genre or category of the document.
        translated (bool): Indicates if the document is a translation.
        page_count (int): Number of pages in the document.
        content_extraction_method (str): Method used for content extraction.
        metadata_extraction_method (str): Method used for metadata extraction.
        full (bool): Indicates if the document is available in complete variant, not just a slice
        restrict_sharing(bool): Indicates if the document is not allowed for sharing and therefore links to it is encrypted
        document_url (str): URL to access the document.
        content_url (str): URL to access the document's content.
        metadata_json: (str): Metadata in JSON-LD format compatible with schema.org. 
        upstream_metadata_url (str): URL to upstream or original metadata source.
    """
    __tablename__ = "https://docs.google.com/spreadsheets/d/1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ/edit?sync_mode=2&gid=2063028338#gid=2063028338"

    md5 = Column(primary_key=True, nullable=False, unique=True, index=True)
    mime_type = Column(String)
    ya_path = Column(String)
    ya_public_url = Column(String)
    ya_public_key = Column(String)
    ya_resource_id = Column(String)
    publisher = Column(String)
    author = Column(String)
    title = Column(String)
    isbn = Column(String)
    publish_date = Column(String)
    language = Column(String)
    genre = Column(String)
    translated = Column(Boolean)
    page_count = Column(Integer)
    content_extraction_method = Column(String)
    metadata_extraction_method = Column(String)
    full = Column(Boolean)
    sharing_restricted=Column(Boolean)
    document_url = Column(String)
    content_url = Column(String)
    metadata_json = Column(String)
    upstream_metadata_url=Column(String)

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        
    def __repr__(self):
        return self.__str__()


class Session:
    def __init__(self, token_file="token.json"):
        self.token_file = token_file
        self._credentials = get_credentials(token_file)
        self._thread_local = threading.local()
        
    def query(self, statement):
        return self._get_session().scalars(statement).all()
    
    def track(self, statement):
        session = self._get_session()
        docs = self.query(statement)
        for doc in docs:
            yield doc
            doc_props = {k: v for k, v in doc.__dict__.items() if k in Document.__table__.columns.keys()}
            statement = update(Document).where(Document.md5.is_(doc.md5))
            statement = statement.values(doc_props)
            session.execute(statement)
        self._flush_session()

    def upsert(self, docs: list[Document]):
        session = self._get_session()
        md5_values = [doc.md5 for doc in docs]
        existing_md5s = set(session.scalars(select(Document.md5).where(Document.md5.in_(md5_values))).all())

        for doc in docs:
            doc_props = {k: v for k, v in doc.__dict__.items() if k in Document.__table__.columns.keys()}
            if doc.md5 in existing_md5s:
                statement = update(Document).where(Document.md5.is_(doc.md5)).values(doc_props)
            else:
                doc_props["md5"] = doc.md5
                statement = insert(Document).values(doc_props)

            session.execute(statement)
        session.commit()
        
    def update(self, doc):
        session = self._get_session()
        doc_props = {k: v for k, v in doc.__dict__.items() if k in Document.__table__.columns.keys()}
        session.execute(update(Document).where(Document.md5.is_(doc.md5)).values(doc_props))
        session.commit()
        
    def delete(self, doc):
        session = self._get_session()
        session.delete(doc)
        session.commit()

    def _flush_session(self):
        """Flushes changes to Google Sheets by closing and resetting the thread-local session."""
        if hasattr(self._thread_local, "session"):
            self._thread_local.session.close()
            del self._thread_local.session

    def _get_session(self):
        if not hasattr(self._thread_local, "session"):
            with open(self.token_file) as f:
                service_account_info = json.load(f)
            engine = create_engine(
                "shillelagh://",
                adapters=["gsheetsapi"],
                adapter_kwargs={
                    "gsheetsapi": {
                        "service_account_info": service_account_info,
                    },
                },
            )
            self._thread_local.session = _Session(engine)

        return self._thread_local.session
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if session := getattr(self._thread_local, "session", None):
            if exc_type:
                session.rollback()
            self._flush_session()