from sqlalchemy import select, text
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session

from credentials import get_credentials
from models import Document


class SessionWrapper():
    def __init__(self, credentials_file="credentials.json", tokens_file="token.json"):
        self.credentials_file = credentials_file
        self.tokens_file = tokens_file
        self.session = None
        self.creds = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def __enter__(self):
        self._create_session()
        return self

    def select(self, stmt):
        self._create_session()
        with self.session.begin():
            return self.session.scalars(stmt).all()


    def _create_session(self):
        creds_updated = False
        if not self.creds or not self.creds.valid:
            self.creds = get_credentials(self.credentials_file, self.tokens_file)
            creds_updated = True

        if creds_updated or not self.session:
            engine = create_engine(
                "shillelagh://",
                adapters=["gsheetsapi"],
                adapter_kwargs={
                    "gsheetsapi": {
                        "access_token": self.creds.token
                    },
                },
            )
            self.session = Session(engine)


if __name__ == "__main__":
    with SessionWrapper() as sw:
        stmt = select(Document).where(Document.md5.is_("4f10abe73cc399ca31c3a9a13932a8f8"))
        for user in sw.select(stmt):
            print(user)
