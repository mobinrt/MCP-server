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
        self.engine = None
        self.SessionLocal = None

    async def init_db(self):
        if self.engine is None:
            self.engine = create_async_engine(
                settings.database_url, echo=True, future=True
            )
            self.SessionLocal = async_sessionmaker(
                bind=self.engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        loop = asyncio.get_running_loop()
        logger.info("DB engine init loop id=%s", id(loop))

    @asynccontextmanager
    async def session_read(self) -> AsyncGenerator[AsyncSession, None]:
        session = self.SessionLocal()
        try:
            try:
                loop = asyncio.get_running_loop()
                logger.debug(
                    "ENTER session_read: loop_id=%s session_id=%s bind_id=%s",
                    id(loop),
                    id(session),
                    id(getattr(session, "bind", None)),
                )
            except RuntimeError:
                logger.debug(
                    "ENTER session_read: no running loop; session_id=%s", id(session)
                )

            yield session

        finally:
            try:
                loop = asyncio.get_running_loop()
                logger.debug(
                    "EXIT session_read: loop_id=%s session_id=%s bind_id=%s",
                    id(loop),
                    id(session),
                    id(getattr(session, "bind", None)),
                )
            except RuntimeError:
                logger.debug(
                    "EXIT session_read: no running loop; session_id=%s", id(session)
                )

            await session.close()

    @asynccontextmanager
    async def session_write(self) -> AsyncGenerator[AsyncSession, None]:
        session = self.SessionLocal()
        try:
            try:
                loop = asyncio.get_running_loop()
                logger.debug(
                    "ENTER session_write: loop_id=%s session_id=%s bind_id=%s",
                    id(loop),
                    id(session),
                    id(getattr(session, "bind", None)),
                )
            except RuntimeError:
                logger.debug(
                    "ENTER session_write: no running loop; session_id=%s", id(session)
                )

            yield session
            await session.commit()

        except Exception:
            await session.rollback()
            raise

        finally:
            try:
                loop = asyncio.get_running_loop()
                logger.debug(
                    "EXIT session_write: loop_id=%s session_id=%s bind_id=%s",
                    id(loop),
                    id(session),
                    id(getattr(session, "bind", None)),
                )
            except RuntimeError:
                logger.debug(
                    "EXIT session_write: no running loop; session_id=%s", id(session)
                )

            await session.close()

    async def get_session_dependency(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.SessionLocal() as session:
            yield session
