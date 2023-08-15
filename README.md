# TPC-H Data Pipeline
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<img src="https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/tpch.jpg" style="width: 100%;height:400px;" align="centre">

## Architecture 
![Pipeline Architecture](https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/DataPipeline.jpg)

Pipeline Breakdown:
 - ELT Job
 - [Python, DuckDB and Pandas](https://github.com/dedyoc/TPCH-Data-Pipeline)
 - [MinIO bucket](https://min.io/)
 - Data Dashboard

### Overview
TPC-H sample data is extracted, transformed, and loaded from MySQL using Python, DuckDB and Pandas into a MinIO bucket as a data lakehouse. A simple dashboard is created using Metabase. The data pipeline tasks are managed using Dagster, ensuring consistent data processing. Data is stored and managed using MinIO as a self-hosted, S3-compatible object storage solution. The data pipeline components are containerized using Docker.

### ELT Flow
![ELT Architecture](https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/elt_dag.png)

 - Data is extracted from MySQL using Python, DuckDB and Pandas.
 - The extracted data is transformed and loaded into a MinIO bucket.
 - The data pipeline tasks are managed using Dagster.
 - Data is stored and managed using MinIO.
 - The data pipeline components are containerized using Docker.

## Data Lakehouse
![Data Lakehouse Architecture](https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/DataLakehouse.png)

## [Metabase Dashboard](https://metabase.com/)
![Metabase Dashboard](https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/Dashboard.gif)

## Reflect
1.) Why use Python, DuckDB and Pandas for ELT?
Python, DuckDB and Pandas provide a powerful and flexible toolkit for data extraction, transformation, and loading.

2.) Why use MinIO for data storage?
MinIO provides a self-hosted, S3-compatible object storage solution, making it an ideal choice for managing and storing data.

## Acknowledgment
This project was inspired by the [Rust Cheaters Data Pipeline](https://github.com/jacob1421/RustCheatersDataPipeline). Special thanks to the developers for their innovative approach to data pipeline architecture.
