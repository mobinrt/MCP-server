import numpy as np
from typing import List, Union
import asyncio
from sentence_transformers import SentenceTransformer

from src.config.settings import settings

_model = None


def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(settings.embedding_model)
    return _model


def embed_texts(
    texts: List[str], batch_size: int | None = None, as_numpy: bool = True
) -> Union[np.ndarray, List[List[float]]]:
    """Embed texts using the model."""

    if not texts:
        return []

    model = get_model()
    bs = batch_size or settings.embedding_batch_size

    all_embs = []
    for i in range(0, len(texts), bs):
        batch = texts[i : i + bs]
        embs = model.encode(batch, show_progress_bar=False)
        all_embs.append(embs)

    embs = np.vstack(all_embs)
    return embs if as_numpy else embs.tolist()


async def embed_texts_async(texts, batch_size=None):
    return await asyncio.to_thread(embed_texts, texts, batch_size)
