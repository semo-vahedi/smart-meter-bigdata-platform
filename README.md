\# Smart Meter Big Data Platform



A reproducible big-data processing pipeline for smart-meter energy data using Python, Apache Kafka, and Apache Spark Structured Streaming.



The project processes historical London Smart Meter observations, replays them as a simulated real-time data stream, and produces AI-ready datasets for energy demand forecasting and household consumption anomaly detection.



\## Project Overview



The platform implements the following processing architecture:



Historical Files → Python Replay Producer → Apache Kafka → Spark Structured Streaming → Data Processing \& Feature Engineering → AI-Ready Datasets



The project separates data engineering from the downstream AI stage. Phase 2 performs the required preprocessing and feature engineering once, allowing Phase 3 models to consume the prepared datasets directly.



\## Main Technologies



\- Python

\- Apache Kafka

\- Apache Spark / PySpark

\- Spark Structured Streaming

\- JupyterLab

\- Parquet

\- WSL2 / Ubuntu

\- Git and GitHub



\## Dataset



The project uses the London Smart Meter Dataset.



Raw dataset characteristics observed during profiling:



\- 167,817,021 half-hourly observations

\- 5,566 households in the raw data

\- 112 half-hourly source files

\- Data period: November 2011 to February 2014



Historical smart-meter observations replayed as a simulated real-time data stream.



The original dataset and large processed datasets are intentionally not stored in this GitHub repository.



\## Data Processing Pipeline



The implemented pipeline includes:



1\. Raw dataset inspection and profiling

2\. Data quality and temporal analysis

3\. AI data-contract definition

4\. Historical data replay using Python

5\. Kafka ingestion

6\. Spark Structured Streaming processing

7\. Data cleaning and transformation

8\. Time-based aggregation

9\. Leakage-safe temporal feature engineering

10\. AI-ready dataset generation

11\. Chronological train/validation/test definition

12\. Phase 3 handoff validation

13\. Performance experiments and reproducibility evidence



\## AI-Ready Outputs



\### Energy Demand Forecasting



The forecasting dataset contains:



\- 19,862 hourly records

\- Aggregated hourly electricity demand

\- Observation and household participation information

\- Temporal features

\- Historical lag features

\- Rolling statistics

\- 24-hour forecasting horizon design



Target:



`total\_energy\_kwh`



\### Household Consumption Anomaly Detection



The anomaly-detection dataset contains:



\- 167,811,461 valid half-hourly observations

\- 5,561 households with valid processed observations

\- Household identifier (`LCLid`)

\- Temporal features

\- Historical lag and change features

\- Rolling consumption statistics



No artificial anomaly labels were introduced during Phase 2.



\## Data Quality



During raw-data profiling:



\- 5,560 invalid energy values were identified and removed

\- 2,001,552 zero-energy observations were preserved

\- Negative energy values: 0

\- Duplicate `(LCLid, timestamp)` observations: 0



The processing policy explicitly distinguishes zero consumption from missing or invalid observations.



\## Leakage Prevention



All forecasting and anomaly-detection features use only information available at the current timestamp or from the past.



Future information is not used for feature generation.



Dataset splitting is chronological rather than random:



\- Training: 70%

\- Validation: 15%

\- Test: 15%



The temporal split definition is included in:



`data/temporal\_split\_contract.json`



\## Performance Experiments



Two mandatory performance experiments were executed using measured results.



\### Data Volume Experiment



| Records | Execution Time | Throughput |

|---:|---:|---:|

| 1,000,000 | 9.558 s | 104,625.27 records/s |

| 5,000,000 | 10.363 s | 482,477.32 records/s |

| 10,000,000 | 10.733 s | 931,671.81 records/s |



\### Replay Rate Experiment



| Requested Rate | Producer Throughput | Streaming Throughput | Delivery |

|---:|---:|---:|---:|

| 100 records/s | 96.25 records/s | 95.64 records/s | 100% |

| 500 records/s | 435.87 records/s | 424.22 records/s | 100% |

| 1,000 records/s | 745.80 records/s | 712.45 records/s | 100% |



The highest measured streaming rate among the tested runs was 712.45 records/s. This represents a measured operating point under the tested configuration, not a claim of maximum system capacity.



\## Repository Structure



```text

smart-meter-bigdata-platform/

├── notebooks/

│   └── London\_Smart\_Meter.ipynb

├── producer/

│   └── replay\_producer.py

├── results/

│   ├── evidence/

│   └── visualizations/

├── docs/

│   └── phase2\_package/

├── data/

│   ├── replay\_manifest.csv

│   └── temporal\_split\_contract.json

├── phase3/

├── spark/

├── .gitignore

└── README.md



Large AI-ready Parquet datasets, Spark checkpoints, temporary streaming data, and runtime artifacts are excluded from GitHub.



Reproducibility



The final Phase 2 environment recorded:



Python 3.11.14

PySpark 4.2.0

kafka-python 3.0.11

Processing environment: WSL2 / Ubuntu

Project storage: mounted Windows E: drive



The repository includes experiment results, evidence files, visualization outputs, replay metadata, and the temporal split contract required to understand and reproduce the processing workflow.



Phase 3



The prepared Phase 2 outputs are designed for direct use in Phase 3 for:



Energy demand forecasting

Household electricity consumption anomaly detection



Phase 3 should consume the AI-ready datasets and the saved temporal split contract without repeating the raw-data preprocessing and feature-engineering pipeline.



Author



Seyed Mohammad Vahedi

