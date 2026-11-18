# Snowflake Python Streaming Data Pipeline

An end-to-end streaming data engineering project demonstrating real-time data ingestion, incremental processing, automated transformations, and analytical reporting using Snowflake and Python.

The project simulates ski resort operations, generating lift ride events, resort tickets, and season pass data to build a continuously updated analytics pipeline.

## Architecture

```text
Python Data Generator
        |
        v
Python Streaming Application
        |
        v
Snowpipe Streaming
        |
        v
Snowflake Raw Tables
        |
        v
Snowflake Streams + Tasks
        |
        v
Dynamic Tables + Snowpark
        |
        v
Hourly / Daily / Weekly Aggregations
        |
        v
Analytical Views + Stored Procedures
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Event generation and streaming application |
| Snowflake | Cloud data platform |
| Snowpipe Streaming | Continuous data ingestion |
| Snowflake Streams | Change data capture |
| Snowflake Tasks | Automated data processing |
| Dynamic Tables | Declarative data transformation pipelines |
| Snowpark Python | DataFrame-based transformations |
| SQL | Data modeling and analytics |
| Docker & Docker Compose | Containerized application execution |
| GitHub Codespaces | Cloud development environment |

## Project Structure

```text
.
├── docker/
│   └── Dockerfile
├── models/
│   ├── lift_ride.py
│   ├── resort_ticket.py
│   └── season_pass.py
├── docker-compose.yml
├── generator.py
├── streamer.py
├── requirements.txt
├── transformation_notebook.ipynb
├── .gitignore
└── README.md
```

## Pipeline Components

### 1. Synthetic Data Generation

Python generates simulated operational events for ski resorts, including:

- Lift ride activity
- Resort ticket transactions
- Season pass information

The generated events provide a continuous data source for testing streaming ingestion and downstream transformations.

### 2. Streaming Ingestion

The Python streaming application sends generated events into Snowflake using Snowpipe Streaming.

This architecture supports continuous ingestion without relying on traditional batch file-loading workflows.

### 3. Incremental Processing

Snowflake Streams capture newly inserted lift ride records.

Snowflake Tasks process these records and populate the `DAILY_VISITS` table.

This separates ingestion from downstream business logic.

### 4. Dynamic Tables

Dynamic Tables maintain derived datasets through automatically scheduled refreshes.

The pipeline includes:

| Dynamic Table | Purpose |
|---|---|
| `HOURLY_LIFT_ACTIVITY` | Hourly lift activity metrics |
| `HOURLY_AMORTIZED_REVENUE` | Hourly revenue allocation |
| `HOURLY_RESORT_SUMMARY` | Combined hourly resort metrics |
| `DAILY_RESORT_SUMMARY` | Daily operational and revenue metrics |
| `WEEKLY_RESORT_SUMMARY` | Weekly resort performance |

### 5. Snowpark Transformations

Snowpark Python is used to implement DataFrame-based transformations directly within Snowflake.

These transformations aggregate operational data into reusable analytical datasets.

### 6. Analytical Reporting

The project includes analytical views:

- `V_DAILY_REVENUE_PERFORMANCE`
- `V_DAILY_NETWORK_METRICS`

It also implements the `GET_RESORT_LIFT_PERFORMANCE` stored procedure to analyze recent lift activity for a selected resort.

The procedure calculates ride counts, unique visitors, activity timestamps, usage rankings, and rides per hour.

## Example Analytics

During the demonstration, the daily network metrics produced the following results:

| Metric | Value |
|---|---:|
| Date | 2026-11-18 |
| Total Network Visitors | 36 |
| Total Network Revenue | 3,060 |
| Total Network Rides | 36 |
| Active Resorts | 2 |
| Average Network Capacity (%) | 0.2 |

These values represent a sample execution rather than production performance.

## Monitoring and Observability

Dynamic Table refresh history can be inspected through Snowflake's `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` function.

During testing, the observed refresh records reported:

- `SUCCEEDED` refresh states
- `SUCCESS` status codes
- `SCHEDULED` refresh triggers
- `NO_DATA` actions when no changes required processing
- A configured target lag of 60 seconds

This provides visibility into the pipeline's automated refresh behavior.

## Running the Project

### Prerequisites

- Snowflake account
- Python environment
- Docker and Docker Compose
- Snowflake credentials and required database permissions

### Configuration

Configure the Snowflake connection credentials and required environment settings for the streaming application.

Keep private keys, passwords, and environment files outside version control.

### Execution

1. Prepare the Snowflake database, schema, roles, and required objects.
2. Configure the Python streaming application.
3. Start the streaming components using Docker Compose.
4. Generate and ingest simulated resort events.
5. Run the Snowflake transformation notebook.
6. Inspect the resulting tables, views, and analytical reports.

The Snowflake notebook contains the transformation and analytical implementation used during the project.

## Key Engineering Concepts

This project demonstrates:

- Event-driven data ingestion
- Streaming pipeline architecture
- Incremental data processing
- Change data capture
- Automated orchestration
- Declarative transformations
- Snowpark-based analytics
- Analytical data modeling
- Pipeline observability

## Security

Sensitive configuration files and credentials are excluded from Git version control through `.gitignore`, including:

- `.env`
- `rsa_key.p8`
- `rsa_key.pub`

## Project Context

This project was developed as a hands-on data engineering portfolio exercise based on a Snowflake streaming pipeline tutorial.

The implementation combines a Python event generator, Snowpipe Streaming, Snowflake Streams and Tasks, Dynamic Tables, Snowpark, and analytical reporting into an end-to-end pipeline.

## Future Improvements

Potential extensions include automated deployment, CI/CD integration, data quality monitoring, and dashboard visualization.

## License

This repository is intended for educational and portfolio demonstration purposes.