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
        city=api_response["location"]["name"],
        country=api_response["location"]["country"],
        temp_c=api_response["current"]["temp_c"],
        localtime=datetime.strptime(
            api_response["location"]["localtime"], "%Y-%m-%d %H:%M"
        ),
        humidity=api_response["current"]["humidity"],
        feelslike_c=api_response["current"]["feelslike_c"],
        wind_kph=api_response["current"]["wind_kph"],
    )
