from typing import (
    Dict,
    Any,
    TypedDict,
)
from datetime import datetime


class RowMeta(TypedDict):
    id: int
    external_id: int
    file_id: int
    embedding_status: str
    vector_id: str
    checksum: str
    content: str
    fields: Dict[str, Any]
    embedding_error: str
    created_at: datetime
    updated_at: datetime


class IncomingRow(TypedDict):
    metadata: RowMeta


class PreparedRow(TypedDict):
    file_id: int
    external_id: int
    content: str
    checksum: str
    fields: Dict[str, Any]


class FileMeta(TypedDict):
    id: int
    last_row_index: int
