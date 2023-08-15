# TPC-H Data Pipeline
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Architecture 
![Pipeline Architecture](https://github.com/dedyoc/TPCH-Data-Pipeline/blob/master/images/architecture.jpg)

Pipeline Breakdown:
 - [TPC-H Sample Data](https://docs.snowflake.com/en/user-guide/sample-data-tpch)
 - DuckDB and Pandas
 - [MinIO bucket](https://min.io/)
 - Metabase Dashboard

### Overview
TPC-H sample data is extracted, transformed, and loaded from MySQL using Python, DuckDB and Pandas into a MinIO bucket as a data lakehouse. A simple dashboard is created using Metabase. The data pipeline tasks are managed using Dagster, ensuring consistent data processing. Data is stored and managed using MinIO as a self-hosted, S3-compatible object storage solution. The data pipeline components are containerized using Docker.

### ELT Flow

 - Data is extracted from MySQL using SQLAlchemy.
 - The extracted data is transformed and loaded into a MinIO bucket using DuckDB.
 - The data pipeline tasks are managed using Dagster.
 - Data is stored and managed using MinIO.
 - The data pipeline components are containerized using Docker.


## Reflect
1.) Why use DuckDB for ELT instead of Pandas or Polars?
DuckDB offers a significant advantage with its SQL interface and its capability as a full OLAP database that supports different Client APIs. This makes it a beginner-friendly choice for my first project going with the data lakehouse architecture.

2.) Why use MinIO for data storage?
MinIO provides a self-hosted, S3-compatible object storage solution, making it an ideal choice for managing and storing data locally.

## Acknowledgment
This project was inspired by the [Build a poor man’s data lake from scratch with DuckDB](https://dagster.io/blog/duckdb-data-lake).