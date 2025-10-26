import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from src.config.logger import logging
from src.config.settings import settings
from src.helpers.singleton import SingletonMeta

logger = logging.getLogger(__name__)

Base = declarative_base()


class Database(metaclass=SingletonMeta):
    def __init__(self):
        self.engine = create_async_engine(settings.database_url, echo=True, future=True)
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        session = self.SessionLocal()
        try:
            yield session

        finally:
            await session.close()

    @asynccontextmanager
    async def session_write(self) -> AsyncGenerator[AsyncSession, None]:
        session = self.SessionLocal()
        try:
            yield session
            await session.commit()

        except Exception:
            await session.rollback()
            raise

        finally:
            await session.close()

    async def get_session_dependency(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.SessionLocal() as session:
            yield session
