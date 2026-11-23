import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="CarbonHive | Industrial Carbon Monitor",
    page_icon="🌱",
    layout="wide"
)

# --------------------------------------------------
# PARQUET DATA SOURCES
# --------------------------------------------------

BASE_PATH = Path(__file__).resolve().parents[1]

LEGACY_PARQUET_PATH = (
    BASE_PATH / "storage" / "parquet" / "telemetry"
)

DYNAMIC_PARQUET_PATH = (
    BASE_PATH / "storage" / "parquet" / "telemetry_dynamic"
)

DYNAMIC_BATCHES_PATH = (
    BASE_PATH / "storage" / "parquet" / "telemetry_dynamic_batches"
)


# --------------------------------------------------
# DATA LOADING
# --------------------------------------------------

@st.cache_data(ttl=10)
def load_data():

    dataframes = []

    # --------------------------------------------------
    # 1. Load legacy Spark output
    # --------------------------------------------------

    if LEGACY_PARQUET_PATH.exists():

        try:
            legacy_df = pd.read_parquet(
                LEGACY_PARQUET_PATH
            )

            legacy_df["_data_source"] = "legacy"
            legacy_df["_record_origin"] = "legacy"
            legacy_df["_source_priority"] = 0

            dataframes.append(legacy_df)

        except Exception as error:
            print(f"Legacy Parquet loading error: {error}")

    # --------------------------------------------------
    # 2. Load original dynamic Spark output
    # --------------------------------------------------

    if DYNAMIC_PARQUET_PATH.exists():

        try:
            dynamic_df = pd.read_parquet(
                DYNAMIC_PARQUET_PATH
            )

            dynamic_df["_data_source"] = "dynamic"
            dynamic_df["_record_origin"] = "dynamic_original"
            dynamic_df["_source_priority"] = 1

            dataframes.append(dynamic_df)

        except Exception as error:
            print(f"Dynamic Parquet loading error: {error}")

    # --------------------------------------------------
    # 3. Load retry-safe dynamic batch output
    # --------------------------------------------------

    if DYNAMIC_BATCHES_PATH.exists():

        batch_files = list(
            DYNAMIC_BATCHES_PATH.rglob("*.parquet")
        )

        for file in batch_files:

            try:
                batch_df = pd.read_parquet(file)

                batch_df["_data_source"] = "dynamic"
                batch_df["_record_origin"] = "dynamic_batch"
                batch_df["_source_priority"] = 2

                dataframes.append(batch_df)

            except Exception as error:
                print(f"Unable to read batch {file}: {error}")

    # --------------------------------------------------
    # 4. Load previous reconciliation output
    # --------------------------------------------------

    previous_reconciled_path = (
        LEGACY_PARQUET_PATH.parent /
        "telemetry_reconciled"
    )

    if previous_reconciled_path.exists():

        try:
            reconciled_df = pd.read_parquet(
                previous_reconciled_path
            )

            reconciled_df["_data_source"] = "dynamic"
            reconciled_df["_record_origin"] = "reconciled_previous"
            reconciled_df["_source_priority"] = 3

            dataframes.append(reconciled_df)

        except Exception as error:
            print(f"Previous reconciliation loading error: {error}")

    # --------------------------------------------------
    # 5. Load Nov 13 reconciliation output
    # --------------------------------------------------

    nov13_reconciled_path = (
        LEGACY_PARQUET_PATH.parent /
        "telemetry_reconciled_nov13"
    )

    if nov13_reconciled_path.exists():

        try:
            nov13_df = pd.read_parquet(
                nov13_reconciled_path
            )

            nov13_df["_data_source"] = "dynamic"
            nov13_df["_record_origin"] = "reconciled_nov13"
            nov13_df["_source_priority"] = 4

            dataframes.append(nov13_df)

        except Exception as error:
            print(f"Nov 13 reconciliation loading error: {error}")

    # --------------------------------------------------
    # 6. Handle empty datasets
    # --------------------------------------------------

    if not dataframes:
        return pd.DataFrame()

    # --------------------------------------------------
    # 7. Combine all available datasets
    # --------------------------------------------------

    df = pd.concat(
        dataframes,
        ignore_index=True,
        sort=False
    )

    # --------------------------------------------------
    # 8. Deduplicate using source precedence
    # --------------------------------------------------

    if "event_id" in df.columns:

        df = (
            df.sort_values(
                "_source_priority",
                ascending=True
            )
            .drop_duplicates(
                subset=["event_id"],
                keep="last"
            )
            .reset_index(drop=True)
        )

    # --------------------------------------------------
    # 9. Identify reconciled records
    # --------------------------------------------------

    df["_is_reconciled"] = (
        df["_record_origin"]
        .astype(str)
        .str.startswith("reconciled")
    )

    # --------------------------------------------------
    # 10. Normalize timestamps
    # --------------------------------------------------

    timestamp_columns = [
        "timestamp",
        "event_timestamp",
        "grid_observation_timestamp",
        "grid_retrieved_at",
        "reconciled_at"
    ]

    for column in timestamp_columns:

        if column in df.columns:

            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True
            )

    # --------------------------------------------------
    # 11. Normalize numeric columns
    # --------------------------------------------------

    numeric_columns = [
        "electricity_kwh",
        "fuel_m3",
        "carbon_emissions_kg",
        "calculated_carbon_intensity_kg_per_unit",
        "production_units",
        "temperature_c",
        "pressure_bar",
        "carbon_intensity_kgco2e_per_kwh",
        "carbon_intensity_gco2e_per_kwh",
        "grid_factor_age_seconds",
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # --------------------------------------------------
    # 12. Normalize Boolean columns safely
    # --------------------------------------------------

    boolean_columns = [
        "detected_anomaly",
        "ground_truth_anomaly",
        "is_synthetic",
        "grid_is_estimated",
        "grid_flow_traced",
    ]

    for column in boolean_columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .astype("string")
                .str.lower()
                .map({
                    "true": True,
                    "false": False,
                    "1": True,
                    "0": False,
                    "1.0": True,
                    "0.0": False
                })
                .fillna(False)
                .astype(bool)
            )

    return df
# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("CarbonHive")
st.subheader("Industrial Carbon Monitoring & Integrity Analytics")

st.caption(
    "Streaming telemetry analytics powered by "
    "Apache Kafka, Apache Spark, and Streamlit."
)

st.divider()


# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

try:
    df = load_data()

except Exception as error:
    st.error(f"Unable to load Parquet data: {error}")
    st.info(
        "Ensure the Spark streaming application has "
        "written data to storage/parquet/telemetry."
    )
    st.stop()


if df.empty:
    st.warning("No telemetry records are available yet.")
    st.stop()


# --------------------------------------------------
# SIDEBAR FILTERS
# --------------------------------------------------

st.sidebar.header("Dashboard Controls")

equipment_options = sorted(
    df["equipment_id"].dropna().unique().tolist()
)

selected_equipment = st.sidebar.multiselect(
    "Select Equipment",
    options=equipment_options,
    default=equipment_options
)

# --------------------------------------------------
# EQUIPMENT FILTER
# --------------------------------------------------

filtered_df = df[
    df["equipment_id"].isin(selected_equipment)
].copy()


# --------------------------------------------------
# DATE RANGE FILTER
# --------------------------------------------------

filtered_df["timestamp"] = pd.to_datetime(
    filtered_df["timestamp"],
    errors="coerce",
    utc=True
)

valid_timestamps = filtered_df["timestamp"].dropna()

if not valid_timestamps.empty:

    min_date = valid_timestamps.min().date()
    max_date = valid_timestamps.max().date()

    st.sidebar.markdown("---")
    st.sidebar.subheader("Time Range")

    selected_dates = st.sidebar.date_input(
        "Select monitoring period",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:

        start_date, end_date = selected_dates

        start_timestamp = pd.Timestamp(start_date, tz="UTC")
        end_timestamp = (
            pd.Timestamp(end_date, tz="UTC")
            + pd.Timedelta(days=1)
        )

        filtered_df = filtered_df[
            (filtered_df["timestamp"] >= start_timestamp)
            & (filtered_df["timestamp"] < end_timestamp)
        ].copy()

else:
    st.sidebar.warning("No valid timestamps available.")
if filtered_df.empty:
    st.warning("No records match the selected equipment.")
    st.stop()

st.sidebar.caption(
    f"Last dashboard refresh: {time.strftime('%H:%M:%S')}"
)

if st.sidebar.button("Refresh Dashboard"):
    st.cache_data.clear()
    st.rerun()

auto_refresh = st.sidebar.checkbox(
    "Auto refresh every 15 seconds",
    value=False
)


# --------------------------------------------------
# KPI METRICS
# --------------------------------------------------

total_events = len(filtered_df)

# Total electricity across all industrial telemetry.
total_electricity = filtered_df[
    "electricity_kwh"
].sum()

# Only dynamically matched records are valid for
# Electricity Maps-based carbon accounting.
dynamic_matched_df = filtered_df[
    (filtered_df["_data_source"] == "dynamic")
    & (filtered_df["grid_match_status"] == "matched")
].copy()

# Count dynamic events awaiting valid grid observations.
pending_events = int(
    (
        (filtered_df["_data_source"] == "dynamic")
        & (
            filtered_df["grid_match_status"]
            == "no_valid_grid_observation"
        )
    ).sum()
)

# Verified electricity consumption.
verified_electricity = dynamic_matched_df[
    "electricity_kwh"
].sum()

# Verified carbon emissions.
total_emissions = dynamic_matched_df[
    "carbon_emissions_kg"
].sum()

# Production-weighted carbon intensity.
valid_intensity_df = dynamic_matched_df[
    (dynamic_matched_df["production_units"] > 0)
    & dynamic_matched_df["carbon_emissions_kg"].notna()
].copy()

total_verified_production = valid_intensity_df[
    "production_units"
].sum()

average_intensity = (
    valid_intensity_df["carbon_emissions_kg"].sum()
    / total_verified_production
    if total_verified_production > 0
    else float("nan")
)

# Anomaly metrics across all telemetry.
anomaly_count = int(
    filtered_df["detected_anomaly"].sum()
)

anomaly_percentage = (
    anomaly_count / total_events * 100
    if total_events else 0
)

# Grid matching coverage.
dynamic_event_count = int(
    (filtered_df["_data_source"] == "dynamic").sum()
)

grid_matching_coverage = (
    len(dynamic_matched_df) / dynamic_event_count * 100
    if dynamic_event_count else 0
)

st.markdown("### Key Performance Indicators")

col1, col2, col3 = st.columns(3)
col4, col5 = st.columns(2)

col1.metric(
    "Total Events",
    f"{total_events:,}"
)

col2.metric(
    "Electricity (All Telemetry)",
    f"{total_electricity:,.2f} kWh"
)

col3.metric(
    "Verified CO₂e Emissions",
    f"{total_emissions:,.2f} kg"
)

col4.metric(
    "Weighted Carbon Intensity",
    (
        f"{average_intensity:.4f} kg/unit"
        if pd.notna(average_intensity)
        else "N/A"
    )
)

col5.metric(
    "Detected Anomalies",
    f"{anomaly_count:,}"
)

st.caption(
    f"Verified electricity: {verified_electricity:,.2f} kWh"
)

st.caption(
    f"Grid matching coverage: {grid_matching_coverage:.2f}% "
    f"({len(dynamic_matched_df):,}/{dynamic_event_count:,} "
    "dynamic events)"
)

st.caption(
    f"Dynamic events awaiting valid grid observations: "
    f"{pending_events:,}"
)

st.caption(
    "Carbon KPIs include only dynamically matched events "
    "with valid Electricity Maps grid observations. "
    "Legacy illustrative calculations are excluded."
)

st.divider()

# --------------------------------------------------
# ANOMALY ANALYTICS
# --------------------------------------------------

st.markdown("### Anomaly Detection Analytics")

left, right = st.columns(2)

anomaly_df = filtered_df[
    filtered_df["detected_anomaly"]
]

with left:
    st.markdown("**Anomaly Category Distribution**")

    category_counts = (
        anomaly_df["detected_anomaly_type"]
        .fillna("Uncategorized")
        .value_counts()
        .rename_axis("Anomaly Type")
        .reset_index(name="Count")
    )

    if not category_counts.empty:
        fig = px.pie(
            category_counts,
            names="Anomaly Type",
            values="Count",
            hole=0.45,
            title="Detected Anomalies by Category"
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No anomalies detected for this selection.")

with right:
    st.markdown("**Normal vs Anomalous Events**")

    status_df = pd.DataFrame({
        "Status": ["Normal", "Anomalous"],
        "Count": [
            total_events - anomaly_count,
            anomaly_count
        ]
    })

    fig = px.bar(
        status_df,
        x="Status",
        y="Count",
        color="Status",
        title="Event Classification",
        text="Count"
    )

    st.plotly_chart(fig, use_container_width=True)


st.divider()

# --------------------------------------------------
# EQUIPMENT-WISE ANOMALY COMPARISON
# --------------------------------------------------

st.markdown("### Equipment-wise Anomaly Comparison")

equipment_anomaly_df = (
    filtered_df
    .groupby("equipment_id", as_index=False)
    .agg(
        total_events=("event_id", "count"),
        anomaly_count=("detected_anomaly", "sum")
    )
)

equipment_anomaly_df["normal_count"] = (
    equipment_anomaly_df["total_events"]
    - equipment_anomaly_df["anomaly_count"]
)

equipment_anomaly_df["anomaly_rate_pct"] = (
    equipment_anomaly_df["anomaly_count"]
    / equipment_anomaly_df["total_events"].replace(0, float("nan"))
    * 100
)

if not equipment_anomaly_df.empty:

    anomaly_comparison = equipment_anomaly_df.melt(
        id_vars="equipment_id",
        value_vars=["normal_count", "anomaly_count"],
        var_name="Event Type",
        value_name="Event Count"
    )

    anomaly_comparison["Event Type"] = (
        anomaly_comparison["Event Type"].map({
            "normal_count": "Normal",
            "anomaly_count": "Anomalous"
        })
    )

    fig = px.bar(
        anomaly_comparison,
        x="equipment_id",
        y="Event Count",
        color="Event Type",
        barmode="group",
        text="Event Count",
        title="Normal vs Anomalous Events by Equipment",
        labels={
            "equipment_id": "Equipment",
            "Event Count": "Number of Events"
        }
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    st.markdown("**Equipment Anomaly Rate Ranking**")

    anomaly_ranking = equipment_anomaly_df.sort_values(
        "anomaly_rate_pct",
        ascending=False
    ).copy()

    anomaly_ranking.insert(
        0,
        "Rank",
        range(1, len(anomaly_ranking) + 1)
    )

    anomaly_ranking["anomaly_rate_pct"] = (
        anomaly_ranking["anomaly_rate_pct"].round(2)
    )

    st.dataframe(
        anomaly_ranking,
        use_container_width=True,
        hide_index=True,
        column_config={
            "anomaly_rate_pct": st.column_config.NumberColumn(
                "Anomaly Rate (%)",
                format="%.2f%%"
            )
        }
    )

else:
    st.info("No equipment data available for anomaly comparison.")

st.divider()

# --------------------------------------------------
# EQUIPMENT ANALYTICS
# --------------------------------------------------

st.markdown("### Equipment-wise Carbon Analytics")

# Summarize all industrial telemetry by equipment.
equipment_summary = (
    filtered_df
    .groupby("equipment_id", as_index=False)
    .agg(
        electricity_kwh=("electricity_kwh", "sum"),
        production_units=("production_units", "sum"),
        event_count=("event_id", "count"),
        anomaly_count=("detected_anomaly", "sum")
    )
)

# Calculate emissions only from dynamically matched records.
dynamic_equipment_summary = (
    dynamic_matched_df
    .groupby("equipment_id", as_index=False)
    .agg(
        emissions_kg=("carbon_emissions_kg", "sum"),
        verified_production=("production_units", "sum")
    )
)

# Merge verified emissions with the complete equipment summary.
equipment_summary = equipment_summary.merge(
    dynamic_equipment_summary,
    on="equipment_id",
    how="left"
)

# Equipment without matched grid observations get zero
# verified emissions in this summary.
equipment_summary["emissions_kg"] = (
    equipment_summary["emissions_kg"].fillna(0)
)

# Production-weighted carbon intensity by equipment.
equipment_summary["carbon_intensity_kg_per_unit"] = (
    equipment_summary["emissions_kg"]
    / equipment_summary["verified_production"].fillna(0).replace(
        0, float("nan")
    )
)

# Equipment anomaly rate.
equipment_summary["anomaly_rate_pct"] = (
    equipment_summary["anomaly_count"]
    / equipment_summary["event_count"].replace(
        0, float("nan")
    )
    * 100
)

# Electricity consumption per verified production unit.
equipment_summary["electricity_per_unit"] = (
    equipment_summary["electricity_kwh"]
    / equipment_summary["production_units"].replace(
        0, float("nan")
    )
)

# Equipment-wise verified carbon emissions chart.
fig = px.bar(
    equipment_summary,
    x="equipment_id",
    y="emissions_kg",
    color="equipment_id",
    text_auto=".2f",
    title="Verified CO₂e Emissions by Equipment",
    labels={
        "equipment_id": "Equipment",
        "emissions_kg": "CO₂e Emissions (kg)"
    }
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# Equipment-wise summary table.
equipment_display = equipment_summary.copy()

equipment_display["electricity_per_unit"] = (
    equipment_display["electricity_per_unit"].round(4)
)

equipment_display["carbon_intensity_kg_per_unit"] = (
    equipment_display["carbon_intensity_kg_per_unit"].round(4)
)

equipment_display["anomaly_rate_pct"] = (
    equipment_display["anomaly_rate_pct"].round(2)
)

st.dataframe(
    equipment_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "electricity_per_unit": st.column_config.NumberColumn(
            "Electricity per Unit (kWh/unit)",
            format="%.4f"
        ),
        "carbon_intensity_kg_per_unit": st.column_config.NumberColumn(
            "Carbon Intensity (kg CO₂e/unit)",
            format="%.4f"
        ),
        "anomaly_rate_pct": st.column_config.NumberColumn(
            "Anomaly Rate (%)",
            format="%.2f%%"
        )
    }
)

# --------------------------------------------------
# EMISSION HOTSPOT ANALYSIS
# --------------------------------------------------

st.markdown("### Emission Hotspot Analysis")

hotspot_df = equipment_summary[
    equipment_summary["emissions_kg"] > 0
].copy()

total_equipment_emissions = hotspot_df["emissions_kg"].sum()

if not hotspot_df.empty and total_equipment_emissions > 0:

    hotspot_df["emission_share_pct"] = (
        hotspot_df["emissions_kg"]
        / total_equipment_emissions
        * 100
    )

    hotspot_df = hotspot_df.sort_values(
        "emissions_kg",
        ascending=False
    )

    highest_emitter = hotspot_df.iloc[0]

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Highest Emission Equipment",
        highest_emitter["equipment_id"]
    )

    col2.metric(
        "Highest Equipment Emissions",
        f"{highest_emitter['emissions_kg']:.3f} kg CO₂e"
    )

    col3.metric(
        "Share of Verified Emissions",
        f"{highest_emitter['emission_share_pct']:.2f}%"
    )

    fig = px.bar(
        hotspot_df,
        x="equipment_id",
        y="emissions_kg",
        color="emission_share_pct",
        text="emission_share_pct",
        title="Equipment Contribution to Verified CO₂e Emissions",
        labels={
            "equipment_id": "Equipment",
            "emissions_kg": "Verified CO₂e Emissions (kg)",
            "emission_share_pct": "Emission Share (%)"
        },
        hover_data={
            "emission_share_pct": ":.2f"
        }
    )

    fig.update_traces(
        texttemplate="%{text:.2f}%",
        textposition="outside"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    st.dataframe(
        hotspot_df[
            [
                "equipment_id",
                "emissions_kg",
                "emission_share_pct",
                "verified_production"
            ]
        ].round(3),
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        "Emission contributions are calculated only from telemetry "
        "with valid, dynamically matched grid carbon observations. "
        "Equipment without matched observations is excluded."
    )

else:
    st.info(
        "No verified equipment emissions are available for hotspot analysis."
    )

st.divider()

# --------------------------------------------------
# EQUIPMENT EFFICIENCY RANKING
# --------------------------------------------------

st.markdown("### Equipment Efficiency Ranking")

efficiency_df = equipment_summary[
    equipment_summary["electricity_per_unit"].notna()
    & (equipment_summary["production_units"] > 0)
].copy()

if not efficiency_df.empty:

    efficiency_df = efficiency_df.sort_values(
        "electricity_per_unit",
        ascending=True
    )

    efficiency_df["efficiency_rank"] = range(
        1, len(efficiency_df) + 1
    )

    fig = px.bar(
        efficiency_df,
        x="equipment_id",
        y="electricity_per_unit",
        color="electricity_per_unit",
        text_auto=".4f",
        title="Equipment Electricity Efficiency Ranking",
        labels={
            "equipment_id": "Equipment",
            "electricity_per_unit": "Electricity Consumption (kWh/unit)"
        },
        hover_data=[
            "production_units",
            "electricity_kwh",
            "anomaly_rate_pct"
        ]
    )

    fig.update_layout(
        xaxis_title="Equipment",
        yaxis_title="Electricity Consumption (kWh/unit)",
        showlegend=False
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    st.caption(
        "Lower electricity consumption per production unit indicates "
        "better electricity efficiency. Comparisons assume comparable "
        "production processes and operating conditions."
    )

    st.dataframe(
        efficiency_df[
            [
                "efficiency_rank",
                "equipment_id",
                "electricity_per_unit",
                "electricity_kwh",
                "production_units",
                "anomaly_rate_pct"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

else:
    st.info(
        "Insufficient production data to calculate equipment efficiency."
    )

st.divider()

# --------------------------------------------------
# INTERACTIVE EQUIPMENT EXPLORER
# --------------------------------------------------

st.markdown("### Interactive Equipment Explorer")

equipment_detail_options = sorted(
    equipment_summary["equipment_id"].dropna().unique().tolist()
)

if equipment_detail_options:

    selected_machine = st.selectbox(
        "Select a machine to inspect",
        equipment_detail_options,
        key="equipment_explorer_selector"
    )

    machine_df = filtered_df[
        filtered_df["equipment_id"] == selected_machine
    ].copy()

    machine_matched_df = dynamic_matched_df[
        dynamic_matched_df["equipment_id"] == selected_machine
    ].copy()

    machine_summary = equipment_summary[
        equipment_summary["equipment_id"] == selected_machine
    ].iloc[0]

    st.markdown(f"#### Machine: {selected_machine}")

    m1, m2, m3, m4 = st.columns(4)

    m1.metric(
        "Total Events",
        f"{int(machine_summary['event_count']):,}"
    )

    m2.metric(
        "Electricity",
        f"{machine_summary['electricity_kwh']:,.2f} kWh"
    )

    m3.metric(
        "Verified CO₂e",
        f"{machine_summary['emissions_kg']:,.2f} kg"
    )

    m4.metric(
        "Anomaly Rate",
        (
            f"{machine_summary['anomaly_rate_pct']:.2f}%"
            if pd.notna(machine_summary["anomaly_rate_pct"])
            else "N/A"
        )
    )

    m5, m6 = st.columns(2)

    m5.metric(
        "Production Units",
        f"{machine_summary['production_units']:,.0f}"
    )

    m6.metric(
        "Carbon Intensity",
        (
            f"{machine_summary['carbon_intensity_kg_per_unit']:.4f} kg/unit"
            if pd.notna(machine_summary["carbon_intensity_kg_per_unit"])
            else "N/A"
        )
    )

    if not machine_matched_df.empty:

        machine_trend = (
            machine_matched_df
            .dropna(subset=["timestamp", "carbon_emissions_kg"])
            .sort_values("timestamp")
        )

        if not machine_trend.empty:

            fig = px.line(
                machine_trend,
                x="timestamp",
                y="carbon_emissions_kg",
                markers=True,
                title=f"{selected_machine} — Verified CO₂e Trend",
                labels={
                    "timestamp": "Timestamp",
                    "carbon_emissions_kg": "CO₂e (kg)"
                }
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

    else:
        st.info(
            "No valid Electricity Maps grid matches are available "
            "for this machine in the selected period."
        )

    st.markdown("**Machine Telemetry Records**")

    st.dataframe(
        machine_df.sort_values("timestamp", ascending=False),
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("No equipment records available for exploration.")

st.divider()

# --------------------------------------------------
# TIME-SERIES ANALYTICS
# --------------------------------------------------

st.markdown("### Carbon Emissions Trend")

# Use only dynamic records with valid grid observations.
time_df = (
    dynamic_matched_df
    .dropna(
        subset=[
            "timestamp",
            "carbon_emissions_kg",
            "calculated_carbon_intensity_kg_per_unit"
        ]
    )
    .sort_values("timestamp")
    .copy()
)

if not time_df.empty:

    # Electricity-related carbon emissions over time.
    fig = px.line(
        time_df,
        x="timestamp",
        y="carbon_emissions_kg",
        color="equipment_id",
        title="Electricity-related CO₂e Emissions Over Time",
        labels={
            "timestamp": "Timestamp",
            "carbon_emissions_kg": "CO₂e Emissions (kg)"
        }
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    # Carbon intensity over time.
    fig = px.line(
        time_df,
        x="timestamp",
        y="calculated_carbon_intensity_kg_per_unit",
        color="equipment_id",
        title="Carbon Intensity Over Time",
        labels={
            "timestamp": "Timestamp",
            "calculated_carbon_intensity_kg_per_unit":
                "Carbon Intensity (kg CO₂e/unit)"
        }
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    st.caption(
        f"Carbon trends are based on {len(time_df):,} "
        "dynamically matched industrial events using "
        "Electricity Maps grid-carbon observations."
    )

else:
    st.info(
        "No dynamically matched records are available "
        "for carbon time-series analysis."
    )

st.divider()


# --------------------------------------------------
# DETECTION EVALUATION
# --------------------------------------------------

st.markdown("### Anomaly Detection Evaluation")

# Evaluate only records with available ground-truth labels.
evaluation_df = filtered_df.dropna(
    subset=[
        "ground_truth_anomaly",
        "detected_anomaly"
    ]
).copy()

if not evaluation_df.empty:

    actual = evaluation_df[
        "ground_truth_anomaly"
    ].astype(bool)

    predicted = evaluation_df[
        "detected_anomaly"
    ].astype(bool)

    tp = int((actual & predicted).sum())
    tn = int((~actual & ~predicted).sum())
    fp = int((~actual & predicted).sum())
    fn = int((actual & ~predicted).sum())

    precision = (
        tp / (tp + fp)
        if (tp + fp) else 0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) else 0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) else 0
    )

    accuracy = (
        (tp + tn) / len(evaluation_df)
    )

    metric1, metric2, metric3, metric4 = st.columns(4)

    metric1.metric("Accuracy", f"{accuracy * 100:.2f}%")
    metric2.metric("Precision", f"{precision * 100:.2f}%")
    metric3.metric("Recall", f"{recall * 100:.2f}%")
    metric4.metric("F1 Score", f"{f1 * 100:.2f}%")

    confusion_df = pd.DataFrame(
        [
            [tn, fp],
            [fn, tp]
        ],
        index=["Actual Normal", "Actual Anomaly"],
        columns=["Predicted Normal", "Predicted Anomaly"]
    )

    st.markdown("**Confusion Matrix**")

    st.dataframe(
        confusion_df,
        use_container_width=True
    )

    st.caption(
        f"Evaluation based on {len(evaluation_df):,} "
        "records with available ground-truth labels. "
        "The telemetry is synthetically generated."
    )

else:
    st.info("No records with ground-truth labels are available for evaluation.")


    st.info(
    """
    **Interpretation of Anomaly Detection Results**

    - **Data source:** Telemetry is synthetically generated by the CarbonHive simulator.
    - **Ground truth:** Anomaly labels are generated by the simulator.
    - **Detection method:** The current anomaly detector uses rule-based thresholds.
    - **Evaluation:** Accuracy, precision, recall, and F1 score measure
      agreement between detected anomalies and simulator-generated labels.
    - **Limitation:** These results do not establish generalization to real-world
      industrial equipment or independently verified anomalies.
    """
)


# --------------------------------------------------
# RAW TELEMETRY
# --------------------------------------------------

with st.expander("View Telemetry Records"):
    st.dataframe(
        filtered_df.sort_values(
            "timestamp",
            ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )


# ==================================================
# CARBONHIVE PIPELINE RESOURCE MONITORING
# ==================================================

st.divider()

st.markdown("## Pipeline Health & Resource Monitoring")

from pathlib import Path
import json

metrics_file = (
    Path(__file__).resolve().parent.parent
    / "results"
    / "system_metrics.jsonl"
)

if metrics_file.exists():

    monitoring_records = []

    with open(metrics_file, "r", encoding="utf-8") as file:

        for line in file:

            try:
                monitoring_records.append(json.loads(line))

            except json.JSONDecodeError:
                continue

    if monitoring_records:

        # ------------------------------------------
        # HOST RESOURCE HISTORY
        # ------------------------------------------

        host_records = []

        container_records = []

        for record in monitoring_records:

            timestamp = pd.to_datetime(
                record.get("timestamp"),
                utc=True,
                errors="coerce"
            )

            host = record.get("host", {})

            if pd.notna(timestamp):

                host_records.append({
                    "timestamp": timestamp,
                    "cpu_percent": host.get("cpu_percent"),
                    "ram_percent": host.get("ram_percent"),
                    "ram_used_gb": host.get("ram_used_gb"),
                    "ram_available_gb": host.get("ram_available_gb")
                })

                for container in record.get(
                    "docker_containers", []
                ):

                    container_records.append({
                        "timestamp": timestamp,
                        "container_name": container.get(
                            "container_name"
                        ),
                        "cpu_percent": container.get(
                            "cpu_percent"
                        ),
                        "memory_percent": container.get(
                            "memory_percent"
                        ),
                        "process_count": container.get(
                            "process_count"
                        ),
                        "status": container.get("status")
                    })

        host_df = pd.DataFrame(host_records)

        container_df = pd.DataFrame(container_records)

        if not host_df.empty:

            host_df = host_df.sort_values("timestamp")

            latest_host = host_df.iloc[-1]

            # --------------------------------------
            # HOST RESOURCE KPIs
            # --------------------------------------

            st.markdown("### Host System Resources")

            h1, h2, h3, h4 = st.columns(4)

            h1.metric(
                "CPU Utilization",
                f"{latest_host['cpu_percent']:.1f}%"
            )

            h2.metric(
                "RAM Utilization",
                f"{latest_host['ram_percent']:.1f}%",
                delta=(
                    "High usage"
                    if latest_host["ram_percent"] >= 85
                    else "Within threshold"
                ),
                delta_color="inverse"
            )

            h3.metric(
                "RAM Used",
                f"{latest_host['ram_used_gb']:.2f} GB"
            )

            h4.metric(
                "RAM Available",
                f"{latest_host['ram_available_gb']:.2f} GB"
            )

            # --------------------------------------
            # HOST CPU HISTORY
            # --------------------------------------

            st.markdown("### Host CPU Utilization")

            fig = px.line(
                host_df,
                x="timestamp",
                y="cpu_percent",
                markers=True,
                title="Historical CPU Utilization",
                labels={
                    "timestamp": "Time",
                    "cpu_percent": "CPU Utilization (%)"
                }
            )

            fig.update_yaxes(
                range=[0, 100]
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

            # --------------------------------------
            # HOST RAM HISTORY
            # --------------------------------------

            st.markdown("### Host Memory Utilization")

            fig = px.line(
                host_df,
                x="timestamp",
                y="ram_percent",
                markers=True,
                title="Historical RAM Utilization",
                labels={
                    "timestamp": "Time",
                    "ram_percent": "RAM Utilization (%)"
                }
            )

            fig.add_hline(
                y=85,
                line_dash="dash",
                annotation_text="85% Warning Threshold"
            )

            fig.update_yaxes(
                range=[0, 100]
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

        # ------------------------------------------
        # DOCKER CONTAINER MONITORING
        # ------------------------------------------

        if not container_df.empty:

            st.markdown("### Docker Container Monitoring")

            container_df = container_df.sort_values(
                "timestamp"
            )

            latest_containers = (
                container_df
                .sort_values("timestamp")
                .groupby("container_name")
                .tail(1)
            )

            st.dataframe(
                latest_containers[
                    [
                        "container_name",
                        "status",
                        "cpu_percent",
                        "memory_percent",
                        "process_count"
                    ]
                ],
                use_container_width=True,
                hide_index=True
            )

            # --------------------------------------
            # CONTAINER CPU COMPARISON
            # --------------------------------------

            st.markdown("### Container CPU Utilization")

            fig = px.line(
                container_df,
                x="timestamp",
                y="cpu_percent",
                color="container_name",
                markers=True,
                title="Spark vs Kafka CPU Utilization",
                labels={
                    "timestamp": "Time",
                    "cpu_percent": "CPU Utilization (%)",
                    "container_name": "Container"
                }
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

            # --------------------------------------
            # CONTAINER MEMORY COMPARISON
            # --------------------------------------

            st.markdown("### Container Memory Utilization")

            fig = px.line(
                container_df,
                x="timestamp",
                y="memory_percent",
                color="container_name",
                markers=True,
                title="Spark vs Kafka Memory Utilization",
                labels={
                    "timestamp": "Time",
                    "memory_percent": "Memory Utilization (%)",
                    "container_name": "Container"
                }
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

        # ------------------------------------------
        # RESOURCE HEALTH STATUS
        # ------------------------------------------

        st.markdown("### Pipeline Resource Health")

        if not host_df.empty:

            latest_ram = latest_host["ram_percent"]
            latest_cpu = latest_host["cpu_percent"]

            if latest_ram >= 90 or latest_cpu >= 95:

                st.error(
                    "CRITICAL: Host resource utilization is very high."
                )

            elif latest_ram >= 85 or latest_cpu >= 80:

                st.warning(
                    "WARNING: Host resource utilization is elevated."
                )

            else:

                st.success(
                    "HEALTHY: Host resource utilization is within limits."
                )

        st.caption(
            f"Historical monitoring observations: "
            f"{len(monitoring_records)}"
        )

        st.caption(
            "Resource metrics represent host and Docker container "
            "utilization. They do not directly measure electricity "
            "consumption or compute-related carbon emissions."
        )

    else:

        st.warning("No valid monitoring records found.")

else:

    st.info(
        "Resource monitoring history is not available yet. "
        "Start the CarbonHive resource monitoring service."
    )

## --------------------------------------------------


# --------------------------------------------------
# EXTERNAL INDUSTRIAL CARBON / ENERGY BENCHMARK
# --------------------------------------------------
st.divider()
st.markdown("## External Industrial Carbon Benchmark vs CarbonHive")
st.markdown(
    "**External source:** IndustryForge 2023 plant-level industrial energy, "
    "production and CO₂ dataset (Zenodo record 23059795). The benchmark contains "
    "European industrial installations across 31 sectors. This comparison filters "
    "steel-sector facilities and compares their annual energy/emissions indicators "
    "with annualized run-rates calculated from CarbonHive's current verified telemetry."
)
st.caption(
    "Important scope note: IndustryForge rows represent annual facility-level steel-industry values; "
    "CarbonHive rows represent simulated equipment telemetry extrapolated from the selected live run. "
    "The charts are contextual benchmarks, not a like-for-like factory validation. "
    "CarbonHive's CO₂e covers electricity only; the external facility CO₂ figure may include broader process/combustion emissions."
)

EXTERNAL_BENCHMARK_PATH = BASE_PATH / "external_data" / "industryforge_steel_benchmark.csv"

@st.cache_data(ttl=300)
def load_industry_benchmark(path_string):
    data = pd.read_csv(path_string)
    data.columns = [str(column).strip() for column in data.columns]
    return data

if EXTERNAL_BENCHMARK_PATH.exists():
    try:
        benchmark_df = load_industry_benchmark(str(EXTERNAL_BENCHMARK_PATH))
        numeric_benchmark_cols = [
            "production_t_per_yr", "electricity_demand_mwh_per_yr", "emissions_t_co2"
        ]
        for col in numeric_benchmark_cols:
            if col in benchmark_df.columns:
                benchmark_df[col] = pd.to_numeric(benchmark_df[col], errors="coerce")

        # Only use CarbonHive's valid dynamic matches for the electricity-carbon comparison.
        current_df = dynamic_matched_df.copy()
        if current_df.empty:
            st.warning(
                "The external benchmark is loaded, but CarbonHive currently has no valid dynamically matched "
                "telemetry rows. Start the grid-carbon producer and Spark stream to populate the comparison."
            )
        else:
            current_df["timestamp"] = pd.to_datetime(current_df["timestamp"], errors="coerce", utc=True)
            current_df = current_df.dropna(subset=["timestamp"])
            duration_hours = (
                (current_df["timestamp"].max() - current_df["timestamp"].min()).total_seconds() / 3600
                if len(current_df) > 1 else 0
            )
            if duration_hours <= 0:
                st.warning("Not enough timestamp span in current verified telemetry to calculate an annualized run-rate.")
            else:
                annualization_factor = (365.25 * 24) / duration_hours
                # Aggregate verified CarbonHive values by equipment and annualize the observed run-rate.
                current_equipment = (
                    current_df.groupby("equipment_id", as_index=False)
                    .agg(
                        observed_electricity_kwh=("electricity_kwh", "sum"),
                        observed_emissions_kg=("carbon_emissions_kg", "sum"),
                        observed_production_units=("production_units", "sum"),
                        observed_events=("event_id", "count"),
                    )
                )
                current_equipment["annualized_electricity_mwh"] = (
                    current_equipment["observed_electricity_kwh"] * annualization_factor / 1000
                )
                current_equipment["annualized_emissions_tco2e"] = (
                    current_equipment["observed_emissions_kg"] * annualization_factor / 1000
                )

                # Benchmark records with the relevant annual fields present.
                steel_energy = benchmark_df.dropna(subset=["electricity_demand_mwh_per_yr"])
                steel_emissions = benchmark_df.dropna(subset=["emissions_t_co2"])
                steel_energy = steel_energy[steel_energy["electricity_demand_mwh_per_yr"] >= 0]
                steel_emissions = steel_emissions[steel_emissions["emissions_t_co2"] >= 0]

                st.markdown("### 1. Annual electricity demand: steel facilities vs CarbonHive equipment run-rate")
                if not steel_energy.empty:
                    external_energy_median = steel_energy["electricity_demand_mwh_per_yr"].median()
                    external_energy_p25 = steel_energy["electricity_demand_mwh_per_yr"].quantile(0.25)
                    external_energy_p75 = steel_energy["electricity_demand_mwh_per_yr"].quantile(0.75)
                    energy_rows = [{
                        "Entity": "External steel-facility median",
                        "Annual electricity (MWh)": external_energy_median,
                    }]
                    for _, row in current_equipment.iterrows():
                        energy_rows.append({
                            "Entity": f"CarbonHive {row['equipment_id']}",
                            "Annual electricity (MWh)": row["annualized_electricity_mwh"],
                        })
                    energy_plot_df = pd.DataFrame(energy_rows)
                    fig = px.bar(
                        energy_plot_df, x="Entity", y="Annual electricity (MWh)",
                        color="Entity", text_auto=".3s",
                        title="External steel-facility annual electricity vs CarbonHive annualized equipment run-rate"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(
                        f"External steel-facility median: {external_energy_median:,.2f} MWh/year "
                        f"(25th–75th percentile: {external_energy_p25:,.2f}–{external_energy_p75:,.2f}; "
                        f"n={len(steel_energy):,}). CarbonHive values are annualized from {duration_hours:.2f} "
                        "hours of verified telemetry. Facility and simulated-equipment scales differ."
                    )
                else:
                    st.info("No valid annual electricity-demand values were found in the external steel benchmark.")

                st.markdown("### 2. Annual CO₂ emissions: external steel facilities vs CarbonHive electricity-related CO₂e")
                if not steel_emissions.empty:
                    external_emissions_median = steel_emissions["emissions_t_co2"].median()
                    external_emissions_p25 = steel_emissions["emissions_t_co2"].quantile(0.25)
                    external_emissions_p75 = steel_emissions["emissions_t_co2"].quantile(0.75)
                    emission_rows = [{
                        "Entity": "External steel-facility median",
                        "Annual emissions (tCO₂/year)": external_emissions_median,
                    }]
                    for _, row in current_equipment.iterrows():
                        emission_rows.append({
                            "Entity": f"CarbonHive {row['equipment_id']}",
                            "Annual emissions (tCO₂/year)": row["annualized_emissions_tco2e"],
                        })
                    emission_plot_df = pd.DataFrame(emission_rows)
                    fig = px.bar(
                        emission_plot_df, x="Entity", y="Annual emissions (tCO₂/year)",
                        color="Entity", text_auto=".3s",
                        title="External facility CO₂ vs CarbonHive annualized electricity-related CO₂e"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(
                        f"External steel-facility median: {external_emissions_median:,.3f} tCO₂/year "
                        f"(25th–75th percentile: {external_emissions_p25:,.3f}–{external_emissions_p75:,.3f}; "
                        f"n={len(steel_emissions):,}). CarbonHive values are annualized from the selected "
                        "verified stream window. Emissions boundaries differ: external facility CO₂ may include "
                        "process and fuel combustion; CarbonHive currently calculates electricity-related CO₂e only."
                    )
                else:
                    st.info("No valid annual CO₂ emissions values were found in the external steel benchmark.")

                st.markdown("### 3. External benchmark profile: steel-facility emissions intensity")
                if {"production_t_per_yr", "emissions_t_co2"}.issubset(benchmark_df.columns):
                    intensity_df = benchmark_df.dropna(subset=["production_t_per_yr", "emissions_t_co2"]).copy()
                    intensity_df = intensity_df[
                        (intensity_df["production_t_per_yr"] > 0) & (intensity_df["emissions_t_co2"] >= 0)
                    ]
                    if not intensity_df.empty:
                        intensity_df["kgCO2_per_t_product"] = (
                            intensity_df["emissions_t_co2"] * 1000 / intensity_df["production_t_per_yr"]
                        )
                        fig = px.histogram(
                            intensity_df, x="kgCO2_per_t_product", nbins=30,
                            title="IndustryForge steel-facility CO₂ intensity distribution",
                            labels={"kgCO2_per_t_product": "kg CO₂ per tonne of product"}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        st.metric(
                            "External steel-facility median emissions intensity",
                            f"{intensity_df['kgCO2_per_t_product'].median():,.2f} kg CO₂/t product"
                        )
                        st.caption(
                            "This external intensity is shown as a benchmark profile only. CarbonHive's production_units "
                            "are simulator units, not tonnes, so its kg CO₂e/unit metric is not plotted on this same axis."
                        )

                # Compact source / methodology summary for review and reproducibility.
                st.markdown("### Benchmark methodology and coverage")
                st.dataframe(pd.DataFrame([
                    ["Dataset", "IndustryForge plant-level industrial energy, production and CO₂ dataset"],
                    ["Year", "2023"],
                    ["Sector selected", "Steel facilities"],
                    ["External measurements", "Annual electricity demand (MWh/year), annual CO₂ (tCO₂/year), production (t/year)"],
                    ["CarbonHive measurements", "Current dynamically matched telemetry; electricity-related CO₂e only"],
                    ["Time alignment", f"CarbonHive observed window: {duration_hours:.2f} hours; annualized run-rate projection"],
                    ["Comparability", "Facility-level annual benchmark vs simulated-equipment run-rate; contextual, not like-for-like validation"],
                ], columns=["Item", "Description"]), use_container_width=True, hide_index=True)
                st.caption(
                    f"External steel-facility rows loaded: {len(benchmark_df):,}. "
                    f"CarbonHive equipment groups compared: {len(current_equipment):,}. "
                    "Source: Berchiche et al., IndustryForge 2023, Zenodo record 23059795."
                )
    except Exception as error:
        st.error(f"Unable to load IndustryForge benchmark: {error}")
        st.info("Run the included external_data/download_industry_benchmark.py script and confirm the CSV is created.")
else:
    st.warning("IndustryForge steel benchmark CSV not found. Download it once using the command below.")
    st.code("python external_data/download_industry_benchmark.py", language="powershell")
    st.caption(
        "The downloader extracts the steel-sector facility records from the public IndustryForge 2023 archive."
    )

# DASHBOARD FOOTER
# --------------------------------------------------

st.divider()

st.caption(
    "CarbonHive | Integrity-Aware Real-Time Industrial "
    "Carbon Monitoring and Analytics Framework"
)

st.caption(
    "Carbon accounting uses Electricity Maps grid-carbon "
    "intensity observations for the IN-SO zone. "
    "Observations may be estimated."
)

st.caption(
    "Industrial telemetry is synthetically generated. "
    "Reported emissions cover electricity consumption only; "
    "fuel-related emissions are not included."
)

st.caption(
    "HMAC-SHA256 is used for telemetry integrity verification. "
    "Anomaly detection currently uses rule-based thresholds."
)