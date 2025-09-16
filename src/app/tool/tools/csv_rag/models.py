from src.base.models import BaseModel
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Text, JSON
from typing import Optional

from src.enum.embedding_status import embeddingStatus


class CSVRow(BaseModel):
    __tablename__ = "csv_rows"

    source_file: Mapped[str] = mapped_column(String(255), index=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(
        String(64), unique=True, index=True, nullable=False
    )
    fields: Mapped[dict] = mapped_column(JSON, nullable=True)
    extra: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    embedding_status: Mapped[embeddingStatus] = mapped_column(
        String(1),
        nullable=False,
        default=embeddingStatus.PENDING.value,
    )
    vector_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    embedding_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class CSVFile(BaseModel):
    __tablename__ = "csv_files"

    path: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
