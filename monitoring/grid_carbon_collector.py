import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


# Load credentials from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY")
ZONE = os.getenv("ELECTRICITY_MAPS_ZONE")

API_URL = (
    "https://api.electricitymaps.com/v4/"
    "carbon-intensity/latest"
)


def fetch_grid_carbon_intensity():
    if not API_KEY:
        raise ValueError(
            "ELECTRICITY_MAPS_API_KEY is missing from .env"
        )

    if not ZONE:
        raise ValueError(
            "ELECTRICITY_MAPS_ZONE is missing from .env"
        )

    params = {
        "zone": ZONE,
        "temporalGranularity": "15_minutes",
        "emissionFactorType": "lifecycle",
        "flowTraced": "true",
    }

    headers = {
        "auth-token": API_KEY,
        "Accept": "application/json",
    }

    print(f"Requesting grid carbon intensity for {ZONE}...")

    response = requests.get(
        API_URL,
        headers=headers,
        params=params,
        timeout=30,
    )

    if response.status_code == 401:
        raise RuntimeError(
            "Authentication failed. Check your API key."
        )

    if response.status_code == 403:
        raise RuntimeError(
            "Access denied. Check your academic API permissions "
            "and zone access."
        )

    if response.status_code == 404:
        raise RuntimeError(
            "No data found for this zone or endpoint."
        )

    response.raise_for_status()

    payload = response.json()

    intensity = payload.get("carbonIntensity")

    if intensity is None:
        raise ValueError(
            "API response does not contain carbonIntensity."
        )

    intensity = float(intensity)

    if intensity < 0:
        raise ValueError(
            "Received an invalid negative carbon intensity."
        )

    result = {
        "source": "Electricity Maps",
        "zone": payload.get("zone"),
        "carbon_intensity_gco2e_per_kwh": intensity,
        "carbon_intensity_kgco2e_per_kwh": intensity / 1000,
        "observation_timestamp": payload.get("datetime"),
        "updated_at": payload.get("updatedAt"),
        "retrieved_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "emission_factor_type": payload.get(
            "emissionFactorType"
        ),
        "flow_traced": payload.get("flowTraced"),
        "is_estimated": payload.get("isEstimated"),
        "estimation_method": payload.get(
            "estimationMethod"
        ),
        "temporal_granularity": payload.get(
            "temporalGranularity"
        ),
    }

    if result["zone"] != ZONE:
        raise ValueError(
            f"Zone mismatch: requested {ZONE}, "
            f"received {result['zone']}"
        )

    output_dir = PROJECT_ROOT / "results"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "grid_carbon_latest.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=4)

    print("\n========== GRID CARBON INTENSITY ==========")
    print(f"Zone: {result['zone']}")
    print(
        "Carbon intensity:",
        f"{intensity:.2f} gCO2e/kWh"
    )
    print(
        "Carbon intensity:",
        f"{intensity / 1000:.6f} kgCO2e/kWh"
    )
    print(
        "Observation time:",
        result["observation_timestamp"]
    )
    print("Estimated:", result["is_estimated"])
    print("Factor type:", result["emission_factor_type"])
    print("Flow traced:", result["flow_traced"])
    print("Granularity:", result["temporal_granularity"])
    print(f"\nSaved response to: {output_file}")

    return result


if __name__ == "__main__":
    try:
        fetch_grid_carbon_intensity()
    except requests.exceptions.Timeout:
        print("ERROR: Electricity Maps API request timed out.")
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to Electricity Maps.")
    except requests.exceptions.HTTPError as error:
        print("HTTP ERROR:", error)
        if error.response is not None:
            print("API response:", error.response.text[:1000])
    except Exception as error:
        print(f"ERROR: {error}")
