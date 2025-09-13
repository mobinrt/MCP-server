import hashlib
from typing import Dict


def row_checksum(values: Dict[str, str]) -> str:
    """Compute a stable checksum for row dict. Use sorted keys to be deterministic."""
    m = hashlib.sha256()
    for k in sorted(values.keys()):
        v = values[k]
        if v is None:
            v = ""

        if isinstance(v, str):
            b = v.encode("utf-8")
        else:
            b = str(v).encode("utf-8")
        m.update(k.encode("utf-8") + b"=" + b + b";")
    return m.hexdigest()

