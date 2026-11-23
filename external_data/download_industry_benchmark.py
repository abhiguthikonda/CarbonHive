"""Download and prepare the IndustryForge 2023 industrial benchmark.

Source: Berchiche et al., Plant-level energy demand, production and CO2
emissions for European industry in 2023, Zenodo record 23059795.
The archive contains the plant master as Parquet. This script extracts the
steel-sector facility rows and writes a compact CSV for CarbonHive.
"""
from pathlib import Path
from io import BytesIO
from urllib.request import Request, urlopen
from zipfile import ZipFile
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TARGET_DIR = ROOT / "external_data"
TARGET_DIR.mkdir(parents=True, exist_ok=True)
TARGET_CSV = TARGET_DIR / "industryforge_steel_benchmark.csv"
ARCHIVE = TARGET_DIR / "industryforge_2023.zip"
URL = "https://zenodo.org/records/23059795/files/industryforge_2023_20260930-0758.zip?download=1"

if TARGET_CSV.exists() and TARGET_CSV.stat().st_size > 0:
    print(f"Benchmark already prepared: {TARGET_CSV}")
    raise SystemExit(0)

print("Downloading IndustryForge 2023 industrial energy and CO2 benchmark from Zenodo...")
request = Request(URL, headers={"User-Agent": "CarbonHive/1.0 (academic project)"})
with urlopen(request, timeout=120) as response:
    archive_bytes = response.read()
ARCHIVE.write_bytes(archive_bytes)
print(f"Downloaded archive: {len(archive_bytes):,} bytes")

with ZipFile(BytesIO(archive_bytes)) as archive:
    parquet_candidates = [
        name for name in archive.namelist()
        if name.lower().endswith("facilities.parquet")
        and "industry/" in name.lower()
    ]
    if not parquet_candidates:
        # Fallback: accept any uniquely named facilities master parquet.
        parquet_candidates = [
            name for name in archive.namelist()
            if name.lower().endswith("facilities.parquet")
        ]
    if not parquet_candidates:
        raise RuntimeError(
            "Could not locate industry/facilities.parquet in the Zenodo archive. "
            "Inspect the archive contents and update the extraction path."
        )
    if len(parquet_candidates) > 1:
        preferred = [p for p in parquet_candidates if "/industry/facilities.parquet" in p.lower()]
        parquet_path = preferred[0] if preferred else parquet_candidates[0]
    else:
        parquet_path = parquet_candidates[0]
    print(f"Reading plant-level file: {parquet_path}")
    facilities = pd.read_parquet(BytesIO(archive.read(parquet_path)))

facilities.columns = [str(c).strip() for c in facilities.columns]
required = ["sector", "facility_id", "production_t_per_yr", "electricity_demand_mwh_per_yr", "emissions_t_co2"]
missing = [c for c in required if c not in facilities.columns]
if missing:
    raise RuntimeError(f"Benchmark is missing required columns: {missing}")

steel = facilities[facilities["sector"].astype(str).str.contains("steel", case=False, na=False)].copy()
if steel.empty:
    raise RuntimeError("No steel-sector facility rows were found in the benchmark.")

keep = required + [c for c in ["country", "product", "year", "emissions_basis", "data_basis", "verified_emissions_t_co2", "reported_emissions_t_co2"] if c in steel.columns]
steel = steel[keep].copy()
for col in ["production_t_per_yr", "electricity_demand_mwh_per_yr", "emissions_t_co2", "verified_emissions_t_co2", "reported_emissions_t_co2"]:
    if col in steel.columns:
        steel[col] = pd.to_numeric(steel[col], errors="coerce")

# Retain source rows, including missing values; the dashboard handles missingness.
steel.to_csv(TARGET_CSV, index=False)
print(f"Prepared {len(steel):,} steel-sector facility rows.")
print(f"Saved benchmark CSV: {TARGET_CSV}")
print("Source: IndustryForge / Zenodo record 23059795, base year 2023.")
print("Fields include annual facility CO2 emissions (tCO2/year), electricity demand (MWh/year), and production (tonnes/year).")
print("Note: this is plant-level annual benchmark data, not machine-level live telemetry.")
