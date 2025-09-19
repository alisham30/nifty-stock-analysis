# Nifty Stock Analysis (PySpark + Docker)

## Overview
End-to-end data engineering project analyzing NIFTY50 stock data.
- Compute shock days (>2% drop) and bounce-back rates
- Built with PySpark inside Docker
- Results saved in Parquet/CSV
- Extensible to SQL queries, dashboards, and CI/CD

## Tech Stack
PySpark, Docker, Jupyter, Parquet, Pandas, (DuckDB/Postgres optional), React/Flask (planned)

## How to Run
```bash
docker build -t nifty-spark -f docker/Dockerfile .
docker run --rm -p 8888:8888 -v "${PWD}:/home/jovyan/work" nifty-spark
