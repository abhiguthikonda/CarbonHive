CarbonHive 🌱



Integrity-Aware Real-Time Industrial Carbon Monitoring System



CarbonHive is a real-time industrial carbon monitoring and analytics platform designed to track electricity-related carbon emissions, detect abnormal industrial energy-consumption patterns, verify telemetry integrity, and identify equipment-level carbon hotspots.



The system combines simulated industrial telemetry, Kafka-based data streaming, HMAC-SHA256 integrity verification, Electricity Maps grid carbon intensity data, Apache Spark Structured Streaming, and an interactive Streamlit dashboard.



It also incorporates an independent IndustryForge steel-industry benchmark dataset to provide an external reference for industrial emissions analysis.



Table of Contents



Project Overview



Problem Statement



Project Objectives



Key Features



System Architecture



Technology Stack



Data Sources



Carbon Calculation Methodology



Dashboard and Visualizations



Project Structure



Installation and Setup



Environment Configuration



Data Integrity Verification



Validation and Testing



Limitations



Future Enhancements



Project Overview



Industrial facilities consume significant amounts of electricity, contributing indirectly to greenhouse gas emissions depending on the carbon intensity of the electricity grid.



Traditional monitoring approaches may not provide sufficiently detailed visibility into equipment-level energy consumption, emission hotspots, abnormal operating patterns, and the reliability of incoming telemetry.



CarbonHive addresses these challenges through a streaming-based monitoring architecture.



The platform:



Generates synthetic industrial equipment telemetry.



Streams telemetry through Apache Kafka.



Verifies incoming data integrity using HMAC-SHA256.



Retrieves grid carbon intensity information through Electricity Maps.



Processes verified streaming data using Apache Spark Structured Streaming.



Calculates electricity-related CO₂-equivalent emissions.



Stores processed results in Parquet format.



Presents analytical insights through an interactive Streamlit dashboard.



Problem Statement



Industrial organizations need better visibility into electricity consumption and associated carbon emissions.



Without continuous monitoring and reliable telemetry verification, it becomes difficult to:



Identify abnormal equipment energy consumption.



Detect equipment contributing disproportionately to emissions.



Monitor changing electricity-related carbon intensity.



Evaluate industrial energy efficiency.



Ensure that incoming telemetry has not been modified.



CarbonHive provides an integrated monitoring and analytics solution to address these challenges.



Project Objectives



Develop a real-time industrial telemetry processing pipeline.



Implement cryptographic integrity verification for incoming data.



Integrate external electricity-grid carbon intensity information.



Calculate electricity-related carbon emissions.



Detect anomalous equipment behavior using rule-based thresholds.



Identify equipment-level emission hotspots.



Compare current monitoring results with independent industrial benchmark data.



Visualize system performance and environmental indicators through a dashboard.



Key Features



1\. Real-Time Industrial Telemetry



A Python-based simulator generates synthetic industrial telemetry for equipment monitoring.



The simulated data represents industrial equipment operating conditions and energy consumption.



2\. Data Integrity Verification



Implements HMAC-SHA256 authentication to verify the integrity and authenticity of telemetry messages using a shared secret.



The pipeline separates incoming telemetry from verified telemetry.



3\. Grid Carbon Intensity Integration



Integrates Electricity Maps data to obtain grid carbon intensity information.



The configured grid region is IN-SO.



4\. Streaming Data Processing



Uses Apache Kafka for message streaming and Apache Spark Structured Streaming for continuous processing.



5\. Carbon Emission Calculation



Calculates electricity-related CO₂-equivalent emissions using energy consumption and grid carbon intensity.



6\. Anomaly Detection



Uses rule-based thresholds to identify abnormal equipment operating and energy-consumption patterns.



7\. Equipment-Level Carbon Analysis



Identifies equipment-level emissions, emission contributions, efficiency indicators, and carbon hotspots.



8\. External Industrial Benchmarking



Uses the IndustryForge steel-industry benchmark dataset to provide independent industrial emissions comparisons.



9\. System Resource Monitoring



Tracks host and container resource utilization, including CPU and memory consumption.



10\. Interactive Dashboard



Provides a Streamlit dashboard containing KPIs, analytical visualizations, equipment comparisons, benchmark analysis, and monitoring indicators.



System Architecture



The CarbonHive architecture follows a streaming-based data processing approach.



flowchart TD

&#x20;   A\["Synthetic Industrial Telemetry"]

&#x20;   B\["Apache Kafka"]

&#x20;   C\["HMAC-SHA256 Verification"]

&#x20;   D\["Verified Telemetry Topic"]

&#x20;   E\["Electricity Maps API"]

&#x20;   F\["Grid Carbon Intensity"]

&#x20;   G\["Apache Spark Structured Streaming"]

&#x20;   H\["Carbon Emission Calculation"]

&#x20;   I\["Parquet Storage"]

&#x20;   J\["Streamlit Dashboard"]

&#x20;   K\["IndustryForge Steel Benchmark"]

&#x20;   L\["System Resource Monitoring"]



&#x20;   A --> B

&#x20;   B --> C

&#x20;   C --> D

&#x20;   E --> F

&#x20;   D --> G

&#x20;   F --> G

&#x20;   G --> H

&#x20;   H --> I

&#x20;   I --> J

&#x20;   K --> J

&#x20;   L --> J



Data Processing Workflow



Stage



Component



Responsibility



1



Python Simulator



Generates synthetic industrial telemetry



2



Apache Kafka



Streams incoming telemetry



3



HMAC-SHA256



Verifies telemetry integrity



4



Verified Kafka Topic



Receives authenticated telemetry



5



Electricity Maps API



Provides grid carbon intensity



6



Apache Spark



Processes streaming data



7



Carbon Calculation



Calculates electricity-related CO₂e



8



Parquet



Stores processed data



9



Streamlit



Displays analytics and monitoring results



10



IndustryForge Dataset



Provides external industrial benchmark information



Technology Stack



Technology



Purpose



Python



Core programming language



Apache Kafka



Real-time message streaming



Apache Spark Structured Streaming



Streaming data processing



Streamlit



Interactive dashboard



Pandas



Data manipulation and analysis



Docker



Containerized infrastructure



Parquet



Processed data storage



HMAC-SHA256



Telemetry integrity verification



Electricity Maps API



Grid carbon intensity information



Git and GitHub



Version control and project hosting



Data Sources



CarbonHive uses two distinct categories of data.



1\. Synthetic Industrial Telemetry



The project generates synthetic industrial telemetry through a Python simulator.



The simulated data is used to demonstrate:



Equipment monitoring.



Electricity consumption analysis.



Anomaly detection.



Equipment-level emissions.



Real-time data processing.



Important: The telemetry represents simulated industrial operations, not measurements collected from physical factory sensors.



2\. IndustryForge Steel Benchmark Dataset



Dataset: IndustryForge 2023 Steel Industry Benchmark



Source: Berchiche et al., Zenodo



Dataset record: https://zenodo.org/records/23059795



The dataset contains:



361 steel-facility records.



12 data columns.



Industrial production information.



Electricity demand information.



Reported and verified emissions information.



Dataset Columns



Column



Description



sector



Industrial sector



facility\_id



Facility identifier



production\_t\_per\_yr



Annual production in tonnes



electricity\_demand\_mwh\_per\_yr



Annual electricity demand in MWh



emissions\_t\_co2



Total reported emissions in tonnes



country



Facility country



product



Industrial product



year



Reporting year



emissions\_basis



Basis of emissions reporting



data\_basis



Basis of the underlying data



verified\_emissions\_t\_co2



Verified emissions



reported\_emissions\_t\_co2



Reported emissions



The dataset is used for external steel-industry benchmarking and emissions-intensity analysis.



The dashboard uses 176 records with complete intensity information for the relevant benchmark visualization.



Why use an external dataset?



An independent benchmark provides industrial context beyond the project's own simulated telemetry. It allows the dashboard to present actual published facility-level benchmark records alongside the monitoring system's simulated results without treating the two datasets as identical.



Carbon Calculation Methodology



CarbonHive calculates electricity-related carbon emissions using grid carbon intensity.



1\. Electricity-Related CO₂e



\[

CO\_2e(kg)=

\\frac{Energy(kWh)\\times GridIntensity(gCO\_2e/kWh)}{1000}

]



Where:



Energy represents electricity consumption in kWh.



Grid intensity represents carbon emissions per kWh of electricity.



1000 converts grams into kilograms.



2\. Carbon Intensity



\[

Carbon\\ Intensity =

\\frac{CO\_2e}{Production}

]



This indicator represents emissions per unit of industrial production.



3\. Anomaly Rate



\[

Anomaly\\ Rate(%) =

\\frac{Anomalous\\ Records}{Total\\ Records}

\\times 100

]



4\. Annualized Emissions



\[

Annualized\\ Emissions =

Observed\\ Emissions

\\times

\\frac{8766}{Observed\\ Hours}

]



8766 represents the average number of hours in a year.



Annualized values are estimates based on the observed monitoring period, not independently measured annual emissions.



Dashboard and Visualizations



The CarbonHive dashboard contains 16 visualizations organized around industrial emissions, equipment performance, anomaly analysis, external benchmarking, and system monitoring.



A. Anomaly Analysis



Anomaly category distribution (donut chart).



Normal versus anomalous record comparison.



Equipment-wise anomaly comparison.



Equipment anomaly rate and ranking.



B. Carbon and Equipment Analysis



Verified CO₂e by equipment.



Equipment carbon hotspot contribution.



Equipment energy efficiency comparison (kWh/unit).



Equipment-level CO₂e trend.



C. Emissions and Grid Analysis



Electricity-related emissions trend.



Grid carbon intensity trend.



D. System Resource Monitoring



Host CPU utilization.



Host RAM utilization.



Container CPU utilization.



Container memory utilization.



E. External Benchmarking



Annual electricity demand and CO₂ emissions benchmarks.



Steel-facility carbon intensity distribution.



The dashboard also includes summary KPIs and monitoring indicators to provide an overview of the system's current analytical results.



Project Structure



The repository is organized into functional modules.



carbonhive/

│

├── dashboard/

│   └── Streamlit dashboard components

│

├── external\_data/

│   └── industryforge\_steel\_benchmark.csv

│

├── integrity/

│   └── HMAC integrity verification

│

├── monitoring/

│   └── Grid carbon and resource monitoring

│

├── producer/

│   └── Synthetic telemetry simulator

│

├── streaming/

│   └── Spark streaming processing

│

├── tests/

│   └── Project tests

│

├── .env.example

├── .gitignore

├── docker-compose.yml

├── requirements.txt

├── audit\_dynamic\_output.py

├── validate\_dynamic\_output.py

└── README.md



Generated output, checkpoints, virtual environments, and sensitive configuration files are kept separate from the source repository.



Installation and Setup



Prerequisites



Install the following before running CarbonHive:



Python 3.10 or later (the development environment uses Python 3.12).



Docker Desktop.



Apache Kafka infrastructure configured through Docker Compose.



Java runtime compatible with the installed Spark/PySpark version.



Git.



1\. Clone the Repository



git clone YOUR\_GITHUB\_REPOSITORY\_URL



Navigate into the project:



cd carbonhive



2\. Create a Virtual Environment



python -m venv .venv



Activate it on Windows PowerShell:



.venv\\Scripts\\Activate.ps1



3\. Install Dependencies



pip install -r requirements.txt



4\. Configure Environment Variables



Create a .env file using .env.example as the template.



Configure the required credentials:



ELECTRICITY\_MAPS\_API\_KEY=your\_api\_key

HMAC\_SECRET=your\_shared\_secret



Do not commit .env or expose API keys and secrets publicly.



5\. Start Infrastructure



Start the Docker services defined in docker-compose.yml:



docker compose up -d



Verify the running containers:



docker compose ps



6\. Start the Application Components



The components must be started in the correct dependency order:



Kafka infrastructure.



HMAC verification consumer.



Synthetic telemetry simulator.



Grid carbon intensity producer.



Resource monitoring service.



Spark Structured Streaming application.



Streamlit dashboard.



Known Python module commands:



python -m integrity.consumer



python -m producer.simulator



python -m monitoring.grid\_carbon\_producer



python -m monitoring.resource\_monitor\_service



Start the Spark application and Streamlit dashboard using their respective entry-point files in the streaming/ and dashboard/ directories.



The exact entry-point filenames should be taken from the repository implementation.



Environment Configuration



CarbonHive uses environment-based configuration to separate application settings and sensitive credentials from source code.



Variable



Purpose



ELECTRICITY\_MAPS\_API\_KEY



API authentication for grid carbon intensity data



HMAC\_SECRET



Shared secret used for HMAC-SHA256 verification



Never publish actual credentials in GitHub.



Data Integrity Verification



Data integrity is an important component of CarbonHive.



The project uses HMAC-SHA256 to verify incoming telemetry messages.



Verification Workflow



The telemetry producer generates a message.



A message authentication code is generated using a shared secret.



The message and its authentication information are transmitted through Kafka.



The verification consumer recalculates the expected HMAC.



The received and calculated authentication codes are compared securely.



Valid telemetry is forwarded to the verified Kafka topic.



Kafka Topics



Topic



Purpose



industrial-telemetry



Incoming simulated telemetry



verified-industrial-telemetry



Integrity-verified telemetry



grid-carbon-intensity



Grid carbon intensity information



This mechanism helps detect unauthorized message modifications and ensures that downstream processing uses verified telemetry.



Validation and Testing



CarbonHive includes scripts for auditing and validating dynamic output.



Available scripts:



python audit\_dynamic\_output.py



python validate\_dynamic\_output.py



These scripts support the project's output auditing and validation workflow.



Additional tests are maintained in the tests/ directory.



Limitations



Industrial telemetry is synthetically generated rather than collected from physical industrial sensors.



Anomaly detection uses predefined rule-based thresholds rather than a trained machine learning model.



The emissions calculations currently focus on electricity-related emissions.



Fuel consumption (fuel\_m3) is excluded from the current CO₂e calculations.



Grid carbon intensity depends on the availability and freshness of external API data.



Grid data older than the configured 15-minute freshness threshold is not treated as current.



Annualized emissions are estimates based on the observed monitoring period.



External steel benchmark data represents annual facility-level records and is not equivalent to live equipment-level telemetry.



Future Enhancements



Potential future development areas include:



Integration with real industrial IoT sensors.



Machine learning-based anomaly detection.



Predictive carbon emission analysis.



Additional industrial sector benchmarks.



Fuel-related and broader Scope 1 and Scope 2 emissions accounting.



Historical emissions forecasting.



Cloud deployment and scalable distributed processing.



Automated sustainability reporting.



Project Significance



CarbonHive demonstrates the integration of real-time data engineering, cryptographic data integrity, environmental analytics, and interactive visualization.



By combining streaming technologies with carbon emission calculations and external industrial benchmarks, the project provides a practical demonstration of how data engineering can support industrial sustainability monitoring.



Project Name: CarbonHive

Domain: Data Engineering | Green Computing | Industrial Sustainability

Primary Language: Python

Dashboard: Streamlit

Streaming Infrastructure: Apache Kafka and Apache Spark Structured Streaming

Integrity Mechanism: HMAC-SHA256

