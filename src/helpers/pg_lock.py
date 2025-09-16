from contextlib import asynccontextmanager
from sqlalchemy import text
import asyncio

@asynccontextmanager
async def advisory_lock(session, key: int, wait: bool = False, retries: int = 3, delay: float = 0.1):
    """
    Async context manager for Postgres advisory locks.
    
    - key: integer lock ID
    - wait: if True, blocks until acquired (uses pg_advisory_lock)
            if False, retries a few times before giving up (pg_try_advisory_lock)
    - retries: number of attempts if wait=False
    - delay: delay between retries (seconds)
    
    Yields True if lock acquired, False otherwise.
    """
    func = "pg_advisory_lock" if wait else "pg_try_advisory_lock"
    acquired = False
    try:
        attempt = 0
        while attempt <= retries:
            res = await session.execute(text(f"SELECT {func}(:k)"), {"k": key})
            acquired = res.scalar()
            if acquired or wait:
                break
            attempt += 1
            await asyncio.sleep(delay)
        yield acquired
    finally:
        if acquired:
            await session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
