# Texas Community College Benchmarking

A data pipeline for benchmarking community colleges in Texas.

## Setup

### Prerequisites

- Python 3.12
- Snowflake
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

### Snowflake

1. **Create a Snowflake account** at [signup.snowflake.com](https://signup.snowflake.com/)
   - Select a cloud provider and region (recommend AWS US East)
   - Note your account URL, username, and password

2. **Run the setup script** to create the database structure:
   - Log into your Snowflake account
   - Open a new SQL worksheet
   - Copy and paste the contents of `setup/snowflake_setup.sql`
   - Execute the entire script (this will create warehouse, database, schemas, and tables)

3. **Configure Snowflake credentials**:
   - Save your credentials securely (you'll need them for dbt and Dagster)
   - Recommended: Use environment variables or a `.env` file (don't commit credentials!)

### dbt

Initialize a dbt project:

```bash
dbt init
```

When prompted:

- **Project name**: `texas_cc_benchmarking`
- **Database**: Choose `1`
- **Account**: Your Snowflake account identifier (e.g., `abcxyz-12345`)
- **Username**: Your Snowflake username
- **Password**: Choose `1` for password authentication, and enter your password
- **Role**: `ACCOUNTADMIN`
- **Warehouse**: `COMPUTE_WH`
- **Database**: `TEXAS_CC`
- **Schema**: `STAGING`
- **Threads**: `4` (default)

**Verify the connection:**

```bash
cd texas_cc_benchmarking
dbt debug
```

You should see "All checks passed!" at the end.

## Data

There are two ways to get the data and load it into Snowflake:

### Option 1: Manual Download and Upload

**Step 1: Download IPEDS data**

```bash
python scripts/download_ipeds.py --years 2020 2021 2022 2023 2024 --filter-texas
```

This will download the IPEDS datasets from 2020 to 2024 and filter for Texas community colleges.

**Step 2: Upload seeds to Snowflake**

`dbt seed` will load the data from the `seeds` directory into the `STAGING` schema in Snowflake. However, it will take a while. So we can use the `upload_to_snowflake.py` script to upload the data to Snowflake.

```bash
python scripts/upload_to_snowflake.py
```

### Option 2: Automated with Dagster

**Step 1: Initialize a Dagster project with dbt integration**

```bash
# From the project root (make sure you're in the parent directory)
cd ..
dagster-dbt project scaffold --project-name dagster_pipelines --dbt-project-dir ./texas_cc_benchmarking
```

This creates a `dagster_pipelines/` folder with:

- Asset definitions
- Resources (for Snowflake connection)
- Configuration files

**Step 2: Configure profiles directory**

The scaffold command may not correctly set the dbt profiles directory. Update `dagster_pipelines/dagster_pipelines/project.py`:

```python
texas_cc_benchmarking_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "texas_cc_benchmarking").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
    profiles_dir=Path.home() / ".dbt",  # Add this line
)
```

**Step 3: Verify Dagster setup:**

```bash
cd dagster_pipelines
dagster dev

# If port 3000 is already in use, specify a different port:
# dagster dev --port 3001
```

The Dagster UI should open at http://localhost:3000 (or your specified port).

**Step 4: Run the pipeline**

```bash
dagster pipeline execute -f dagster_pipelines/dagster_pipelines/assets.py
```

This will download the IPEDS datasets from 2020 to 2024 and filter for Texas community colleges.
