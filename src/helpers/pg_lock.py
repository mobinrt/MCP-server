from contextlib import asynccontextmanager
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio

from src.config.logger import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def advisory_lock(session: AsyncSession, key: int, wait: bool = False, retries: int = 3, delay: float = 0.1):
    func = "pg_advisory_lock" if wait else "pg_try_advisory_lock"
    acquired = False

    # log current loop
    try:
        loop = asyncio.get_running_loop()
        logger.debug("advisory_lock loop id=%s key=%s", id(loop), key)
    except RuntimeError:
        logger.debug("advisory_lock: no running loop for key=%s", key)

    # use session.execute to avoid separate `connect()` and cross-connection races
    await session.execute(text("SET LOCAL synchronous_commit TO OFF"))
    try:
        attempt = 0
        while attempt <= retries:
            res = await session.execute(text(f"SELECT {func}(:k)"), {"k": key})
            # different SQLAlchemy versions use different scalar APIs:
            try:
                acquired = res.scalar_one()
            except Exception:
                acquired = res.scalar()
            if acquired or wait:
                break
            attempt += 1
            await asyncio.sleep(delay)
        yield acquired
    finally:
        if acquired:
            try:
                await session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
            except Exception:
                pass
