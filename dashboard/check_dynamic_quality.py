from dashboard.app import load_data

df = load_data()

dynamic = df[df["_data_source"] == "dynamic"].copy()

print("\n========== DYNAMIC DATA QUALITY ==========")

print("Total dynamic records:", len(dynamic))

print("\nGrid match status:")
print(dynamic["grid_match_status"].value_counts(dropna=False))

print("\nCarbon emissions null count:")
print(dynamic["carbon_emissions_kg"].isna().sum())

print("\nGrid factor null count:")
print(dynamic["carbon_intensity_kgco2e_per_kwh"].isna().sum())

print("\nGrid observation timestamp null count:")
print(dynamic["grid_observation_timestamp"].isna().sum())

print("\nRecords by grid match status and carbon availability:")

print(
    dynamic.groupby("grid_match_status", dropna=False)
    .agg(
        records=("event_id", "count"),
        carbon_nulls=("carbon_emissions_kg", lambda x: x.isna().sum()),
        average_grid_age=("grid_factor_age_seconds", "mean")
    )
)

print("\n========== AUDIT COMPLETE ==========")
