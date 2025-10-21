from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from src.config.settings import settings
from src.helpers.singleton import SingletonMeta

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

    def session(self) -> AsyncSession:
        return self.SessionLocal()

    async def get_session_dependency(self) -> AsyncSession:
        async with self.SessionLocal() as session:
            yield session


