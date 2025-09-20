import json
import aiohttp
from typing import Dict, Any
from rapidfuzz import process, fuzz
from pathlib import Path

from src.config.settings import settings
from src.config.logger import logging
from src.base.base_tool import BaseTool
from .schema import parse_weather, WeatherArgs
from . import cities_path

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
        path = Path(self.cities_path) if self.cities_path else cities_path

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
        result = process.extractOne(user_input, city_names, scorer=fuzz.WRatio)
        if not result:
            logger.warning(f"No fuzzy match found for '{user_input}'")
            return None
        if len(result) == 2:
            best_match, score = result
        else:
            best_match, score, _ = result

        logger.info(f"Fuzzy match for '{user_input}' -> '{best_match}' (score={score})")
        return self.name_to_city.get(best_match.lower()) if score > 70 else None

    async def run(self, args: dict) -> dict:
        try:
            parsed = WeatherArgs(**args)
            city = parsed.city.strip()

            city_info = self.name_to_city.get(city.lower()) or self._guess_city(
                city.lower()
            )
            if not city_info:
                return {"error": f"City '{city}' not found in index."}

            # Build request params
            params = {
                "q": f"{city_info['name']},ir",
                "APPID": self.api_key,
                "units": "metric",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        logger.error(
                            "Weather API failed: %s, body=%s", resp.status, text
                        )
                        return {"error": f"Weather API failed: {resp.status}, {text}"}

                    try:
                        data = json.loads(text)
                    except Exception:
                        logger.exception("Failed to parse weather API response")
                        return {"error": "Invalid response from weather API"}

            return parse_weather(data).model_dump()

        except Exception as e:
            logger.exception("WeatherTool failed")
            return {"error": str(e)}
