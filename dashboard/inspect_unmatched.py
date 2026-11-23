from dashboard.app import load_data
import pandas as pd

df = load_data()

unmatched = df[
    (df["_data_source"] == "dynamic") &
    (df["grid_match_status"] == "no_valid_grid_observation")
].copy()

print("\n========== UNMATCHED RECORD INSPECTION ==========")

print("\nTimestamp range:")
print("Earliest:", unmatched["event_timestamp"].min())
print("Latest:", unmatched["event_timestamp"].max())

print("\nRecords by equipment:")
print(unmatched["equipment_id"].value_counts())

print("\nSample unmatched records:")
columns = [
    "event_id",
    "timestamp",
    "equipment_id",
    "electricity_kwh",
    "grid_match_status",
    "grid_observation_timestamp",
    "grid_factor_age_seconds",
    "carbon_emissions_kg"
]

print(unmatched[columns].head(10).to_string(index=False))

print("\n========== INSPECTION COMPLETE ==========")
