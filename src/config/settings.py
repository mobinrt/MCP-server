import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    app_env: str = os.getenv("APP_ENV", "development")
    host: str = os.getenv("APP_HOST", "0.0.0.0")
    port: int = os.getenv("APP_PORT", 8000)
    api_key: str = os.getenv("API_KEY", "changeme")
    
    #csv
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    batch_size: int = int(os.getenv("BATCH_SIZE", "64"))
    embedding_batch_size: int = int(os.getenv("EMBEDDING_BATCH_SIZE", "128"))

    #chromadb
    chroma_persist_directory: str = os.getenv("CHROMA_PERSIST_DIR", "chroma_data")
    chroma_collection_name: str = os.getenv("CHROMA_COLLECTION", "csv_rag_collection")
    chroma_telemetry_enabled: str = os.getenv("CHROMA_TELEMETRY_ENABLED", "True")
    
    #Database
    database_url: str = os.getenv("DATABASE_URL", "changeme")

settings = Settings()