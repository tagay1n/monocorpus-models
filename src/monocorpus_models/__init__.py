import os

import google.auth.transport.requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from sqlalchemy import Column, Integer, DateTime, String, Boolean
from sqlalchemy import select, insert, update
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session as _Session
from sqlalchemy.sql import func
from sqlalchemy.pool import NullPool
import threading


# The OAuth 2.0 scopes we need.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']


def get_credentials(credentials_file='credentials.json', token_file='token.json'):
    """Obtain OAuth 2.0 credentials or refresh token if expired."""

    creds = None

    # Load token if it exists
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Refresh the access token using the refresh token
            creds.refresh(google.auth.transport.requests.Request())
        else:
            # If no valid credentials available, initiate OAuth2 login
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(access_type='offline', prompt='consent')

        # Save the credentials for future use
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    if not creds.refresh_token:
        raise ValueError('No refresh token found in credentials')
    return creds


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "https://docs.google.com/spreadsheets/d/1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ/edit?sync_mode=2&gid=2063028338#gid=2063028338"

    md5 = Column(primary_key=True, nullable=False, unique=True, index=True)
    mime_type = Column(String)
    file_name = Column(String)
    ya_public_url = Column(String)
    ya_public_key = Column(String)
    ya_resource_id = Column(String)
    publisher = Column(String)
    author = Column(String)
    title = Column(String)
    age_limit = Column(String)
    isbn = Column(String)
    publish_date = Column(String)
    summary = Column(String)
    sources = Column(String)
    language = Column(String)
    genre = Column(String)
    translated = Column(Boolean)
    page_count = Column(Integer)
    extraction_complete = Column(Boolean)
    edition = Column(String)
    audience = Column(String)
    content_extraction_method = Column(String)
    metadata_extraction_method = Column(String)
    udc = Column(String)
    bbc = Column(String)
    full = Column(Boolean)
    document_url = Column(String)
    metadata_url = Column(String)
    content_url = Column(String)
    upstream_metadata_url=Column(String)
    created_at = Column(DateTime, default=func.now())
    

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        
    def __repr__(self):
        return self.__str__()


class Session:
    def __init__(self, credentials_file="credentials.json", tokens_file="token.json"):
        self._credentials = get_credentials(credentials_file, tokens_file)
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

    def _flush_session(self):
        """Flushes changes to Google Sheets by closing and resetting the thread-local session."""
        if hasattr(self._thread_local, "session"):
            self._thread_local.session.close()
            del self._thread_local.session

    def _get_session(self):
        # Refresh token only if needed
        if self._credentials.expired and self._credentials.refresh_token:
            print("refreshing token")
            self._credentials.refresh(google.auth.transport.requests.Request())

            # If session already exists, its engine still uses the old token
            # So we must recreate the session with the new token
            if hasattr(self._thread_local, "session"):
                print("deleting expired session")
                self._thread_local.session.commit()
                self._thread_local.session.close()
                del self._thread_local.session

        if not hasattr(self._thread_local, "session"):
            print("creating new session")
            engine = create_engine(
                "shillelagh://",
                adapters=["gsheetsapi"],
                adapter_kwargs={
                    "gsheetsapi": {
                        "access_token": self._credentials.token
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
            
            
if __name__ == "__main__":
    session = Session()
    stmt = select(Document).limit(1)
    doc = session.query(stmt)
    print(doc)