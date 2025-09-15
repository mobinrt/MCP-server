from contextlib import asynccontextmanager


@asynccontextmanager
async def advisory_lock(session, key: int, wait: bool = False):
    """
    Async context manager for Postgres advisory locks.
    - key: integer lock ID
    - wait: if True, blocks until acquired; else returns immediately.
    Yields True if lock acquired, False otherwise.
    """
    func = "pg_advisory_lock" if wait else "pg_try_advisory_lock"
    acquired = False
    try:
        res = await session.execute(f"SELECT {func}(:k)", {"k": key})
        acquired = res.scalar()
        yield acquired
    finally:
        if acquired:
            await session.execute("SELECT pg_advisory_unlock(:k)", {"k": key})
