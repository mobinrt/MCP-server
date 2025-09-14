import os
import requests
import logging
import json
from typing import Dict
from dotenv import load_dotenv
from rapidfuzz import process, fuzz

from src.base.base_tool import BaseTool
from . import CITY_JSON


logger = logging.getLogger(__name__)

load_dotenv()


class WeatherTool(BaseTool):
    def __init__(self) -> None:
        self.api_key = os.getenv("WEATHER_API_KEY")
        self.base_url = os.getenv("WEATHER_URL")
        with open(CITY_JSON, "r", encoding="utf-8") as f:
            self.cities = json.load(f)

        self.name_to_city = {c["name"].lower(): c for c in self.cities}
        
    @property
    def name(self) -> str:
        return "weather"

    @property
    def description(self) -> str:
        return "Fetches current weather information for a given city."

    def _guess_city(self, user_input: str, limit: int = 3):
        """Return best matching city using fuzzy search."""
        city_names = [c["name"] for c in self.cities]
        best_match, score, _ = process.extractOne(
            user_input, city_names, scorer=fuzz.WRatio
        )
        logger.info(f"Fuzzy match for '{user_input}' -> '{best_match}' (score={score})")
        if score > 70:
            return self.name_to_city[best_match.lower()]
        return None