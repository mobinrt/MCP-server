from pydantic import BaseModel


class CsvQuery(BaseModel):
    query: str
    top_k: int = 5


class CsvIngest(BaseModel):
    folder_path: str
    batch_size: int = 512


class WeatherQuery(BaseModel):
    city: str
