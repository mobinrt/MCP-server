from gc import enable
from src.base.models import BaseModel
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Text, JSON, ForeignKey, Boolean
from typing import Optional
from src.enum.csv_status import EmbeddingStatus, FileStatus
from src.enum.executor import Executor

class CSVRow(BaseModel):
    __tablename__ = "csv_rows"
    external_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    file_id: Mapped[int] = mapped_column(
        ForeignKey("csv_files.id", ondelete="CASCADE"), nullable=False
    )

    embedding_status: Mapped[EmbeddingStatus] = mapped_column(
        String(1),
        nullable=False,
        default=EmbeddingStatus.PENDING.value,
    )
    vector_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    embedding_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    checksum: Mapped[str] = mapped_column(
        String(64), unique=True, index=True, nullable=False
    )
    fields: Mapped[dict] = mapped_column(JSON, nullable=True)

    file = relationship("CSVFile", back_populates="rows")

    def to_dict(self):
        return {
            "id": self.id,
            "external_id": self.external_id,
            "file_id": self.file_id,
            "embedding_status": self.embedding_status,
            "vector_id": self.vector_id,
            "checksum": self.checksum,
            "content": self.content,
            "fields": self.fields,
            "embedding_error": self.embedding_error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class CSVFile(BaseModel):
    __tablename__ = "csv_files"

    path: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[FileStatus] = mapped_column(
        String(1),
        nullable=False,
        default=FileStatus.PENDING.value,
    )
    last_row_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    rows = relationship("CSVRow", back_populates="file", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "status": self.status,
            "path": self.path,
            "last_row_index": self.last_row_index,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

class ToolRegistry(BaseModel):
    __tablename__ = "tool_registry"

    name: Mapped[str] = mapped_column(String, unique=True, index=True)  
    description: Mapped[str] = mapped_column(String, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False, default="csv_rag")
    adapter: Mapped[str] = mapped_column(String, nullable=False, default=Executor.IN_PROCESS.value) 
    file_id: Mapped[int] = mapped_column(ForeignKey("csv_files.id", ondelete="CASCADE"))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    file = relationship("CSVFile", backref="tool_entry")
    
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "adapter": self.adapter,
            "file_id": self.file_id,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
