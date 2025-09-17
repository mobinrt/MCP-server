from src.base.models import BaseModel
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Text, JSON, ForeignKey
from typing import Optional
from src.enum.embedding_status import EmbeddingStatus


class CSVRow(BaseModel):
    __tablename__ = "csv_rows"

    file_id: Mapped[int] = mapped_column(
        ForeignKey("csv_files.id", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[int] = mapped_column(
        Integer, index=True, nullable=False
    )

    content: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(
        String(64), unique=True, index=True, nullable=False
    )
    fields: Mapped[dict] = mapped_column(JSON, nullable=True)
    extra: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    embedding_status: Mapped[EmbeddingStatus] = mapped_column(
        String(1),
        nullable=False,
        default=EmbeddingStatus.PENDING.value,
    )
    vector_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    embedding_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    file = relationship("CSVFile", back_populates="rows")


class CSVFile(BaseModel):
    __tablename__ = "csv_files"

    path: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[EmbeddingStatus] = mapped_column(
        String(1),
        nullable=False,
        default=EmbeddingStatus.PENDING.value,
    )
    last_row_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    rows = relationship("CSVRow", back_populates="file", cascade="all, delete-orphan")
