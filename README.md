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

You'll build the actual data pipelines here later.
