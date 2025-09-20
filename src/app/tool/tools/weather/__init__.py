from pathlib import Path
import os
# PROJECT_ROOT = Path(__file__).resolve().parents[3]

# CITY_JSON = PROJECT_ROOT / "static" / "json" / "iran_cities.json"

cities_path = os.getenv(
        "CITIES_JSON_PATH", os.path.join(os.getcwd(), "static/json/iran_cities.json")
    )