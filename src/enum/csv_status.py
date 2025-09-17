from enum import Enum


class EmbeddingStatus(Enum):
    PENDING = "p"
    DONE = "d"
    FAILED = "f"


class FileStatus(Enum):
    PENDING = "p"
    DONE = "d"
    FAILED = "f"
