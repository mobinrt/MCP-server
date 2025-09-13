from src.base.models import BaseModel
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Text, JSONB
from typing import Optional

class CSVRow(BaseModel):
    __tablename__ = "csv_rows"

    source_file: Mapped[str] = mapped_column(String(255), index=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    fields: Mapped[dict] = mapped_column(JSONB, nullable=True)
    extra: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
