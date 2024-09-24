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
    __tablename__ = "https://docs.google.com/spreadsheets/d/1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ/edit#gid=2063028338"

    md5 = Column(primary_key=True, nullable=False, unique=True, index=True)
    mime_type = Column(String)
    names = Column(String)
    ocr = Column(String)
    ya_public_url = Column(String)
    ya_public_key = Column(String)
    text_extracted = Column(Boolean)
    annotation_completed = Column(Boolean)
    sent_for_annotation = Column(Boolean)
    language = Column(String)
    genre = Column(String)
    translated = Column(Boolean)
    pages_count = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    completeness = Column(Integer)

    def __repr__(self):
        """
        Represents every property of the Document object
        :return: string representation of the Document object
        """
        return f"Document(md5={self.md5}, mime_type={self.mime_type}, names={self.names}, ocr={self.ocr}, " \
               f"ya_public_url={self.ya_public_url}, ya_public_key={self.ya_public_key}, text_extracted={self.text_extracted}, " \
               f"annotation_completed={self.annotation_completed}, sent_for_annotation={self.sent_for_annotation}, " \
               f"language={self.language}, genre={self.genre}, translated={self.translated}, " \
               f"pages_count={self.pages_count}, created_at={self.created_at}, completeness={self.completeness})"


class Session:
    def __init__(self, credentials_file="credentials.json", tokens_file="token.json"):
        self.credentials_file = credentials_file
        self.tokens_file = tokens_file
        self.session = None
        self.creds = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def __enter__(self):
        return self

    def select(self, stmt):
        with self._create_session() as s:
            return s.scalars(stmt).all()

    def upsert(self, doc):
        with self._create_session() as s:
            doc_props = {k: v for k, v in doc.__dict__.items() if k in Document.__table__.columns.keys()}
            if s.scalars(select(Document).where(Document.md5.is_(doc.md5)).limit(1)).one_or_none():
                stmt = update(Document).where(Document.md5.is_(doc.md5))
            else:
                stmt = insert(Document)
                doc_props["md5"] = doc.md5

            stmt = stmt.values(doc_props)
            s.execute(stmt)
            s.commit()

    def _create_session(self):
        if not self.creds or not self.creds.valid:
            self.creds = get_credentials(self.credentials_file, self.tokens_file)

        return _Session(
            create_engine(
                "shillelagh://",
                adapters=["gsheetsapi"],
                adapter_kwargs={
                    "gsheetsapi": {
                        "access_token": self.creds.token
                    },
                },
            )
        )
