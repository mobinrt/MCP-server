import numpy as np
from typing import List, Union, Dict, Any
import asyncio
from sentence_transformers import SentenceTransformer
from numpy.linalg import norm

from src.config.settings import settings

_model = None


def get_model():
    """Lazy load embedding model"""
    global _model
    if _model is None:
        _model = SentenceTransformer(settings.embedding_model)
    return _model


def prepare_text_for_embedding(row: Dict[str, Any]) -> str:
    """
    Convert a CSV row into a structured text string for embedding.
    Filters out noise fields like IDs, phone numbers, URLs.
    """
    ignore_keys = {
        "id",
        "external_id",
        "phone",
        "phone_number",
        "map_link",
        "url",
        "link",
        "number",
    }
    parts = []

    for k, v in row.items():
        if not v:
            continue
        if k.lower() in ignore_keys:
            continue
        if isinstance(v, (int, float)):
            continue
        parts.append(f"{k}: {v}")

    return " | ".join(parts)


def embed_texts(
    texts: List[str], batch_size: int | None = None, as_numpy: bool = True
) -> Union[np.ndarray, List[List[float]]]:
    """Embed texts using the model with optional normalization."""

    if not texts:
        return []

    model = get_model()
    bs = batch_size or settings.embedding_batch_size

    all_embs = []
    for i in range(0, len(texts), bs):
        batch = texts[i : i + bs]
        embs = model.encode(batch, show_progress_bar=False, convert_to_numpy=True)
        # Normalize for cosine similarity
        embs = embs / norm(embs, axis=1, keepdims=True)
        all_embs.append(embs)

    embs = np.vstack(all_embs)
    return embs if as_numpy else embs.tolist()


async def embed_texts_async(texts, batch_size=None):
    """Async wrapper"""
    return await asyncio.to_thread(embed_texts, texts, batch_size)
