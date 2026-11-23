<div align="center">

# 🌱 CarbonHive
### Integrity-Aware Real-Time Industrial Carbon Monitoring System

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Security](https://img.shields.io/badge/Integrity-HMAC--SHA256-00C853?style=for-the-badge&logo=shield&logoColor=white)](#data-integrity-verification)

**Real-time telemetry streaming, cryptographic data verification, dynamic grid carbon intensity tracking, and equipment-level hotspot analytics.**

[Project Overview](#-project-overview) • [Architecture](#-system-architecture) • [Methodology](#-carbon-calculation-methodology) • [Visualizations](#-dashboard--visualizations) • [Installation](#-installation--setup)

</div>

---

## 📖 Project Overview

Industrial facilities consume massive amounts of electricity, generating indirect greenhouse gas (Scope 2) emissions that fluctuate alongside the carbon intensity of the regional power grid. Traditional monitoring systems often lack granular visibility into equipment-level hotspots, real-time anomaly detection, and cryptographic assurance that incoming sensor telemetry hasn't been tampered with.

**CarbonHive** bridges data engineering and green computing through an end-to-end streaming architecture:

* **Simulates** industrial equipment telemetry and energy consumption in real time.
* **Streams** high-throughput events via **Apache Kafka**.
* **Authenticates** every payload using **HMAC-SHA256** cryptographic signatures before processing.
* **Enriches** telemetry with live grid carbon intensity from the **Electricity Maps API** (`IN-SO` region).
* **Computes** real-time $\text{CO}_2\text{e}$ emissions and rule-based anomalies in **Apache Spark Structured Streaming**.
* **Persists** curated analytics in columnar **Parquet** format and benchmarks performance against the **IndustryForge** steel-industry dataset inside an interactive **Streamlit** dashboard.

---

## 🎯 Problem Statement & Objectives

### The Challenge
Without continuous, tamper-proof monitoring, industrial operators struggle to:
1. Pinpoint anomalous energy spikes across individual machines.
2. Identify equipment contributing disproportionately to total carbon output.
3. Account for real-time fluctuations in regional grid carbon intensity.
4. Guarantee that incoming telemetry streams are authentic and unmodified.

### Core Objectives
- [x] Build a fault-tolerant, real-time industrial telemetry processing pipeline.
- [x] Enforce cryptographic message authentication (`HMAC-SHA256`) prior to stream analytics.
- [x] Dynamically join live grid carbon intensity with equipment power consumption.
- [x] Detect operational anomalies and rank equipment-level carbon hotspots.
- [x] Contextualize facility performance against an external steel-industry benchmark.

---

## ✨ Key Features

| Feature | Description |
| :--- | :--- |
| **⚡ Real-Time Telemetry Simulator** | Python-based generator producing realistic industrial equipment operating metrics and kWh consumption. |
| **🔐 HMAC-SHA256 Integrity Gate** | Cryptographically signs and verifies every message using a shared secret, isolating unverified data from downstream analytics. |
| **🌍 Live Grid Intensity Integration** | Fetches real-time grid carbon intensity ($\text{gCO}_2\text{e/kWh}$) via Electricity Maps (`IN-SO` zone). |
| **🌊 Distributed Stream Processing** | Combines Apache Kafka and Spark Structured Streaming for continuous stateful transformations. |
| **🏭 Hotspot & Anomaly Detection** | Flags abnormal consumption patterns via rule-based thresholds and ranks equipment by emission share. |
| **📊 External Industry Benchmarking** | Compares simulated output against 176 complete records from the **IndustryForge** steel benchmark dataset. |
| **🖥️ System Resource Telemetry** | Tracks host and Docker container CPU/RAM utilization alongside environmental KPIs. |
| **📈 16-Chart Interactive Dashboard** | Multi-tab Streamlit application covering emissions, anomalies, grid trends, benchmarks, and system health. |

---

## 🏗️ System Architecture

```mermaid
flowchart LR
    subgraph Ingestion ["1. Telemetry & Security"]
        A["🏭 Python Telemetry<br/>Simulator"] --> B[("📨 Apache Kafka<br/>(industrial-telemetry)")]
        B --> C{"🔐 HMAC-SHA256<br/>Verification Consumer"}
        C -- "Valid Signature" --> D[("📨 Verified Topic<br/>(verified-industrial-telemetry)")]
    end

    subgraph Enrichment ["2. External Context"]
        E["🌍 Electricity Maps API<br/>(Zone: IN-SO)"] --> F[("📨 Kafka Topic<br/>(grid-carbon-intensity)")]
    end

    subgraph Processing ["3. Stream Analytics & Storage"]
        D --> G["⚡ Apache Spark<br/>Structured Streaming"]
        F --> G
        G --> H["🧮 CO₂e & Anomaly<br/>Engine"]
        H --> I[("💾 Parquet<br/>Data Lake")]
    end

    subgraph Presentation ["4. Analytics & Monitoring"]
        I --> J["📊 Streamlit<br/>Interactive Dashboard"]
        K["📑 IndustryForge<br/>Steel Benchmark"] --> J
        L["🖥️ Host & Container<br/>Resource Monitor"] --> J
    end

    style C fill:#1b4332,stroke:#2d6a4f,color:#fff
    style G fill:#7f2704,stroke:#d9480f,color:#fff
    style J fill:#800f2f,stroke:#c9184a,color:#fff
