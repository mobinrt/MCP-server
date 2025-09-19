import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from typing import Optional
from src.enum.executor import Executor

load_dotenv()


class Settings(BaseSettings):
    app_env: str = os.getenv("APP_ENV", "development")
    host: str = os.getenv("APP_HOST", "0.0.0.0")
    port: int = os.getenv("APP_PORT", 8000)
    api_key: str = os.getenv("API_KEY", "changeme")

    # csv
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
    batch_size: int = int(os.getenv("BATCH_SIZE", "64"))
    embedding_batch_size: int = int(os.getenv("EMBEDDING_BATCH_SIZE", "128"))

    # chromadb
    chroma_persist_directory: str = os.getenv("CHROMA_PERSIST_DIR", "chroma_data")
    chroma_collection_name: str = os.getenv("CHROMA_COLLECTION", "csv_rag_collection")
    chroma_telemetry_enabled: str = os.getenv("CHROMA_TELEMETRY_ENABLED", "True")

    # Database
    database_url: str = os.getenv("DATABASE_URL", "changeme")

    # Celery
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    celery_result_expires: int = int(os.getenv("CELERY_RESULT_EXPIRES", "3600"))

    worker_task_soft_time_limit: int = int(
        os.getenv("WORKER_TASK_SOFT_TIME_LIMIT", "300")
    )
    worker_task_time_limit: int = int(os.getenv("WORKER_TASK_TIME_LIMIT", "360"))

    worker_ingest_soft_time_limit: int = int(
        os.getenv("WORKER_INGEST_SOFT_TIME_LIMIT", str(300 * 4))
    )
    worker_ingest_time_limit: int = int(
        os.getenv("WORKER_INGEST_TIME_LIMIT", str(360 * 6))
    )
    worker_ingest_max_retries: int = int(os.getenv("WORKER_INGEST_MAX_RETRIES", "3"))

    worker_concurrency: int = int(os.getenv("WORKER_CONCURRENCY", "2"))
    worker_max_tasks_per_child: int = int(
        os.getenv("WORKER_MAX_TASKS_PER_CHILD", "100")
    )

    worker_max_retries: int = int(os.getenv("WORKER_MAX_RETRIES", "3"))
    celery_rate_limit: Optional[str] = os.getenv("CELERY_RATE_LIMIT", None)

    # Adapter timeout
    tool_celery_timeout: int = int(os.getenv("TOOL_CELERY_TIMEOUT", "300"))

    # Executor
    tool_executor: str = os.getenv("TOOL_EXECUTOR", Executor.IN_PROCESS.value).lower()
    env_key:str = os.getenv("ENV_KEY", default=None) #base url

settings = Settings()
