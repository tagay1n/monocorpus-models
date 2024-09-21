from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "https://docs.google.com/spreadsheets/d/1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ/edit#gid=2063028338"

    md5: Mapped[str] = mapped_column(primary_key=True)
    mime_type: Mapped[str] = mapped_column()
    names: Mapped[str] = mapped_column()
    ocr: Mapped[str] = mapped_column()
    ya_public_url: Mapped[str] = mapped_column()
    ya_public_key: Mapped[str] = mapped_column()
    text_extracted: Mapped[str] = mapped_column()
    annotation_completed: Mapped[str] = mapped_column()
    sent_for_annotation: Mapped[str] = mapped_column()
    language: Mapped[str] = mapped_column()
    genre: Mapped[str] = mapped_column()
    translated: Mapped[bool] = mapped_column()
    pages_count: Mapped[int] = mapped_column()

    def __repr__(self):
        return f"<Document(md5={self.md5}, mime_type={self.mime_type}, names={self.names}, ocr={self.ocr}, ya_public_url={self.ya_public_url}, ya_public_key={self.ya_public_key}, text_extracted={self.text_extracted}, annotation_completed={self.annotation_completed}, sent_for_annotation={self.sent_for_annotation}, language={self.language}, genre={self.genre}, translated={self.translated}, pages_count={self.pages_count})>"
