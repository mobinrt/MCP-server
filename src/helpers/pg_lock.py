from contextlib import asynccontextmanager
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio

@asynccontextmanager
async def advisory_lock(session: AsyncSession, key: int, wait: bool = False, retries: int = 3, delay: float = 0.1):
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
    
    async with session.bind.connect() as conn:
        await conn.execute(text("SET LOCAL synchronous_commit TO OFF"))

        try:
            attempt = 0
            while attempt <= retries:
                res = await conn.execute(text(f"SELECT {func}(:k)"), {"k": key})
                acquired = res.scalar()
                if acquired or wait:
                    break
                attempt += 1
                await asyncio.sleep(delay)
            yield acquired
        finally:
            if acquired:
                try:
                    await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
                except Exception:
                    pass