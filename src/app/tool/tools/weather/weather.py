import json
import aiohttp
from typing import Dict, Any
from rapidfuzz import process, fuzz
from pathlib import Path

from src.config.settings import settings
from src.config.logger import logging
from src.base.base_tool import BaseTool
from .schema import parse_weather, WeatherArgs
from . import CITY_JSON

logger = logging.getLogger(__name__)


class WeatherTool(BaseTool):
    def __init__(self, cities_path: str = None) -> None:
        self.api_key = settings.weather_api_key
        self.base_url = settings.weather_url
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

    async def run(self, args: dict) -> dict:
        try:
            parsed = WeatherArgs(**args)
            city_info = self.name_to_city.get(parsed.city.lower()) or self._guess_city(parsed.city.lower())
            if not city_info:
                return {"error": f"City '{parsed.city.lower()}' not found."}

            params = {"name": city_info["name"], "appid": self.api_key, "units": "metric"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url=self.base_url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        return {"error": f"Weather API failed: {resp.status}, {text}"}
                    data = await resp.json()

            return parse_weather(data).model_dump()
        except Exception as e:
            logger.exception("WeatherTool failed")
            return {"error": str(e)}
