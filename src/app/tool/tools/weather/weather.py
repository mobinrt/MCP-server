import requests
from typing import Dict
from dotenv import load_dotenv
import os

load_dotenv()

class WeatherTool:
    """
    Weather Tool using OpenWeather API.
    """
    def __init__(self) -> None:
        self.api_key =  
        self.base_url