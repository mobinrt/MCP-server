from pydantic import BaseModel
from datetime import datetime


class WeatherArgs(BaseModel):
    city: str


class WeatherExtract(BaseModel):
    city: str
    country: str
    temp_c: float
    localtime: datetime
    humidity: float
    feelslike_c: float
    wind_kph: float


def parse_weather(api_response: dict) -> WeatherExtract:
    """
    Based on OpenWeather response
    """
    return WeatherExtract(
        city=api_response["name"],
        country=api_response["sys"]["country"],
        temp_c=api_response["main"]["temp"],
        localtime=datetime.fromtimestamp(api_response["dt"]),
        humidity=api_response["main"]["humidity"],
        feelslike_c=api_response["main"]["feels_like"],
        wind_kph=api_response["wind"]["speed"] * 3.6,
    )
