import os
import json
import aiohttp
from typing import Dict, Any
from dotenv import load_dotenv
from rapidfuzz import process, fuzz
from pathlib import Path

from src.config.logger import logging
from src.base.base_tool import BaseTool
from .schema import parse_weather
from . import CITY_JSON

logger = logging.getLogger(__name__)
load_dotenv()


class WeatherTool(BaseTool):
    def __init__(self, cities_path: str = None) -> None:
        self.api_key = os.getenv("WEATHER_API_KEY")
        self.base_url = os.getenv("WEATHER_URL")
        self.cities_path = cities_path
        self.cities = []
        self.name_to_city = {}
        self._ready = False

    @property
    def name(self) -> str:
        return "weather"

    @property
    def description(self) -> str:
        return "Fetches current weather information for a given city."

    async def initialize(self):
        path = Path(self.cities_path) if self.cities_path else CITY_JSON

        if not path.exists():
            logger.warning(
                "Cities file not found at %s — continuing without index", path
            )
            self._ready = True
            return

        with open(path, "r", encoding="utf-8") as f:
            self.cities = json.load(f)

        self.name_to_city = {c["name"].lower(): c for c in self.cities}
        self._ready = True
        logger.info("WeatherTool initialized with %d cities", len(self.cities))

    def _guess_city(self, user_input: str):
        city_names = [c["name"] for c in self.cities]
        best_match, score, _ = process.extractOne(
            user_input, city_names, scorer=fuzz.WRatio
        )
        logger.info(f"Fuzzy match for '{user_input}' -> '{best_match}' (score={score})")
        return self.name_to_city.get(best_match.lower()) if score > 70 else None

    async def run(self, city: str) -> Dict[str, Any]:
        city_info = self.name_to_city.get(city.lower()) or self._guess_city(city)
        if not city_info:
            raise ValueError(f"City '{city}' not found in Iran city list.")

        params = {"name": city_info["name"], "appid": self.api_key, "units": "metric"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self.base_url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("Weather API error: %s %s", resp.status, text)
                    raise RuntimeError(f"Weather API failed: {resp.status}")
                data = await resp.json()

        return parse_weather(data).model_dump()
