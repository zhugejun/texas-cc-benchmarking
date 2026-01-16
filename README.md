# Texas Community College Benchmarking

A data pipeline for benchmarking community colleges in Texas.

## Overview

This project uses a modern data stack:

```
IPEDS Data → Dagster → Snowflake (RAW) → dbt → Snowflake (MARTS) → Analytics
```

### Architecture

**Data Flow:**

1. **Extract & Load (Dagster)**
   - Downloads IPEDS CSV files from NCES
   - Processes data using pandas
   - Loads to Snowflake `RAW_IPEDS` schema

2. **Transform (dbt)**
   - **Staging**: Clean and standardize raw data (views)
   - **Intermediate**: Business logic and joins (ephemeral CTEs)
   - **Marts**: Final analytics tables (materialized tables)

3. **Schemas**
   - `RAW_IPEDS`: Raw IPEDS data (managed by Dagster)
   - `STAGING`: Cleaned and typed data (dbt views)
   - `INTERMEDIATE`: Reusable business logic (dbt ephemeral)
   - `MARTS`: Analytics-ready tables (dbt tables)

### Data Sources

This project uses IPEDS (Integrated Postsecondary Education Data System) data from NCES:

- **HD**: Institutional Characteristics
- **C_A**: Completions by Award Level
- **EFFY**: 12-Month Enrollment
- **GR**: Graduation Rates
- **SFA**: Student Financial Aid

## Quick Start

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

### Dagster

Initialize a Dagster project with dbt integration:

```bash
# From the project root (make sure you're in the parent directory)
cd ..
dagster-dbt project scaffold --project-name dagster_pipelines --dbt-project-dir ./texas_cc_benchmarking
```

This creates a `dagster_pipelines/` folder with:

- Asset definitions
- Resources (for Snowflake connection)
- Configuration files

**Configure profiles directory:**

The scaffold command may not correctly set the dbt profiles directory. Update `dagster_pipelines/dagster_pipelines/project.py`:

```python
texas_cc_benchmarking_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "texas_cc_benchmarking").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
    profiles_dir=Path.home() / ".dbt",  # Add this line
)
```

**Verify Dagster setup:**

```bash
cd dagster_pipelines
dagster dev

# If port 3000 is already in use, specify a different port:
# dagster dev --port 3001
```

The Dagster UI should open at http://localhost:3000 (or your specified port).

**Note:** Dagster automatically uses your dbt profile credentials from `~/.dbt/profiles.yml`, so no additional configuration is needed!

### 4. Run the Pipeline

Once everything is configured, you can run the complete data pipeline through Dagster:

1. **Start Dagster UI:**

   ```bash
   cd dagster_pipelines
   dagster dev
   ```

2. **Open the UI** at http://localhost:3000

3. **Run the pipeline:**
   - Navigate to **Assets** in the left sidebar
   - You'll see two groups:
     - **ipeds_ingestion**: Downloads and loads raw IPEDS data to Snowflake (partitioned by year)
     - **dbt_transformations**: Runs dbt models to transform the data
   - Each IPEDS asset is **partitioned by year** (2019-2024)

**Download data for specific years:**

- Click on any asset (e.g., `ipeds_institutional_characteristics`)
- You'll see partitions for each year: `2019`, `2020`, `2021`, `2022`, `2023`, `2024`
- Select the years you want to download (e.g., `2020-2024` for the last 5 years)
- Click **Materialize selected partitions**

**Download all available years:**

- Select all assets in the ipeds_ingestion group
- Click **Materialize all partitions**
- This will download all years (2019-2024) for all datasets

The pipeline will:

1. Download IPEDS CSV files from NCES for selected years
2. Extract data and load to Snowflake `RAW_IPEDS` schema
3. Multiple years are stored in the same table with a `YEAR` column
4. Run dbt transformations (staging → intermediate → marts)
5. Create analytics-ready tables in the `MARTS` schema

**Add more years:**

To add more years to the partition definition, edit `dagster_pipelines/dagster_pipelines/assets.py`:

```python
ipeds_years_partitions = StaticPartitionsDefinition(
    ["2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]  # Add years as needed
)
```

## Project Structure

```
texas-community-college-benchmarking/
├── setup/
│   └── snowflake_setup.sql       # Snowflake initialization script
├── texas_cc_benchmarking/        # dbt project
│   ├── models/
│   │   ├── staging/              # Staging models (views)
│   │   ├── intermediate/         # Intermediate models (ephemeral)
│   │   └── marts/                # Final tables (materialized)
│   ├── dbt_project.yml
│   └── ...
├── dagster/                      # Dagster pipelines (to be created)
├── README.md
└── pyproject.toml
```

## Development

### Working with dbt

```bash
# Navigate to dbt project
cd texas_cc_benchmarking

# Test connection
dbt debug

# Create and run models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Next Steps

Now that the data pipeline is set up, the next steps are to build out the dbt transformation layer:

1. **Create dbt sources** - Define source tables in `models/sources.yml` that point to the RAW_IPEDS tables
2. **Build staging models** - One model per source table to clean and standardize the data
3. **Build intermediate models** - Business logic, calculations, and joins
4. **Build marts** - Final analytics tables for Texas community college benchmarking
5. **Add tests** - Data quality tests and documentation
6. **Schedule pipeline** - Set up regular refreshes in Dagster (e.g., weekly, monthly)
