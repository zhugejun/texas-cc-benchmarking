# Texas Community College Benchmarking

A data pipeline for benchmarking community colleges in Texas.

## Setup

### Prerequisites

- Python 3.12
- Snowflake [30-day free tier](https://signup.snowflake.com/)
- dbt
- dagster

### Python

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
```

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv init
uv sync
uv add dbt-snowflake dagster-snowflake dagster-webserver dagster-dbt dagster pandas requests
```

### dbt

```bash

```
