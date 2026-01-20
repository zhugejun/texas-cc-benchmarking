import io
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset, StaticPartitionsDefinition
from dagster_dbt import DbtCliResource, dbt_assets

from .project import texas_cc_benchmarking_project
from .resources import SnowflakeResource


# Define year partitions for IPEDS data (2020-2024)
ipeds_years_partitions = StaticPartitionsDefinition(
    ["2020", "2021", "2022", "2023", "2024"]
)

# Base URL for IPEDS data (pre-2023)
IPEDS_BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data"

# HTTP headers for requests
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# IPEDS dataset configurations
IPEDS_DATASETS = {
    "hd": {
        "name": "HD",
        "description": "Institutional Characteristics",
        "table": "HD",
    },
    "c_a": {
        "name": "C_A",
        "description": "Completions by Award Level",
        "table": "C_A",
    },
    "effy": {
        "name": "EFFY",
        "description": "12-Month Enrollment",
        "table": "EFFY",
    },
    "gr": {
        "name": "GR",
        "description": "Graduation Rates",
        "table": "GR",
    },
    "sfa": {
        "name": "SFA",
        "description": "Student Financial Aid",
        "table": "SFA",
    },
    "ef_d": {
        "name": "EF_D",
        "description": "Retention Rates",
        "table": "EF_D",
    },
}


def get_table_name(dataset_code: str, year: int) -> str:
    """
    Get the IPEDS table name based on dataset code and year.

    Different datasets have different naming conventions:
    - HD, EFFY, GR: {dataset}{year} (e.g., HD2023)
    - C_A: C{year}_A (e.g., C2023_A)
    - SFA: SFA{prev_year}{curr_year} (e.g., SFA2324 for academic year 2023-24)
    - EF_D: EF{year}D (e.g., EF2024D)
    """
    if dataset_code == "C_A":
        return f"C{year}_A"
    elif dataset_code == "SFA":
        prev_year = str(year - 1)[-2:]
        curr_year = str(year)[-2:]
        return f"SFA{prev_year}{curr_year}"
    elif dataset_code == "EF_D":
        return f"EF{year}D"
    else:
        # Standard format: HD2023, EFFY2023, GR2023
        return f"{dataset_code}{year}"


def download_ipeds_file(
    dataset_code: str,
    year: int,
    context: AssetExecutionContext,
) -> pd.DataFrame:
    """
    Download and extract an IPEDS dataset.

    Args:
        dataset_code: The IPEDS dataset code (e.g., 'HD', 'EFFY', 'C_A', 'SFA', 'EF_D')
        year: The year to download (e.g., 2023)
        context: Asset execution context for logging

    Returns:
        DataFrame containing the IPEDS data with YEAR column added
    """
    table_name = get_table_name(dataset_code, year)

    # For 2023+, use the new data-generator API
    if year >= 2023:
        timestamp = str(int(time.time() * 1000))
        url = f"https://nces.ed.gov/ipeds/data-generator?year={year}&tableName={table_name}&HasRV=0&type=csv&t={timestamp}"
        encoding = "utf-8"
    else:
        # For pre-2023, use the old ZIP format
        url = f"{IPEDS_BASE_URL}/{table_name}.zip"
        encoding = "latin-1"

    context.log.info(f"Downloading {table_name} from {url}")

    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=300)
        response.raise_for_status()

        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]

            if not csv_files:
                raise ValueError(f"No CSV file found in {table_name} download")

            csv_filename = csv_files[0]
            context.log.info(f"Extracting {csv_filename} from ZIP archive")

            with z.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file, encoding=encoding, low_memory=False)

        # Add YEAR column
        df["YEAR"] = year

        context.log.info(f"Successfully downloaded {table_name}: {len(df):,} rows, {len(df.columns)} columns")
        return df

    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to download {table_name}: {str(e)}")
        raise
    except zipfile.BadZipFile as e:
        context.log.error(f"Invalid ZIP file for {table_name}: {str(e)}")
        raise


def load_to_snowflake(
    df: pd.DataFrame,
    table_name: str,
    snowflake: SnowflakeResource,
    context: AssetExecutionContext,
    year: int = None,
) -> None:
    """
    Load a DataFrame to Snowflake using schema inference.

    Uses INFER_SCHEMA to dynamically create/replace tables based on CSV structure.

    Args:
        df: DataFrame to load
        table_name: Target table name
        snowflake: Snowflake resource
        context: Asset execution context for logging
        year: Year of data being loaded (for partition tracking)
    """
    context.log.info(f"Loading {len(df):,} rows to Snowflake table {table_name}" +
                     (f" for year {year}" if year else ""))

    conn = snowflake.get_connection()
    cursor = conn.cursor()

    try:
        # Create file formats if they don't exist
        context.log.info("Ensuring file formats exist...")

        # Format for INFER_SCHEMA - needs PARSE_HEADER to read column names
        cursor.execute("""
            CREATE FILE FORMAT IF NOT EXISTS ipeds_csv_infer
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                PARSE_HEADER = TRUE
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL')
                ENCODING = 'UTF8'
        """)

        # Format for COPY INTO - needs SKIP_HEADER to skip header row when loading
        cursor.execute("""
            CREATE FILE FORMAT IF NOT EXISTS ipeds_csv_load
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL')
                ENCODING = 'UTF8'
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """)

        # Write DataFrame to temporary CSV
        csv_filename = f"{table_name.lower()}_{year}.csv" if year else f"{table_name.lower()}.csv"
        temp_csv = Path(f"/tmp/{csv_filename}")
        df.to_csv(temp_csv, index=False, encoding="utf-8")

        # Step 1: Upload file to user stage
        context.log.info(f"Uploading {csv_filename} to staging area...")
        cursor.execute(f"PUT file://{temp_csv} @~/staged AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # Step 2: Infer schema and create table
        context.log.info(f"Creating table {table_name} with inferred schema...")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@~/staged/{csv_filename}',
                        FILE_FORMAT => 'ipeds_csv_infer'
                    )
                )
            )
        """)

        # Step 3: Load data
        context.log.info(f"Loading data into {table_name}...")
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM '@~/staged/{csv_filename}'
            FILE_FORMAT = ipeds_csv_load
            ON_ERROR = 'CONTINUE'
        """)

        # Step 4: Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        # Step 5: Clean up staged file and temp file
        cursor.execute(f"REMOVE @~/staged/{csv_filename}")
        temp_csv.unlink()

        context.log.info(f"Successfully loaded {row_count:,} rows to {table_name}")

    finally:
        cursor.close()
        conn.close()


# Create partitioned assets for each IPEDS dataset
@asset(
    name="ipeds_institutional_characteristics",
    description="IPEDS Institutional Characteristics (HD) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_hd(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Institutional Characteristics data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("HD", year, context)
    load_to_snowflake(df, "HD", snowflake, context, year)


@asset(
    name="ipeds_completions",
    description="IPEDS Completions (C_A) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_c_a(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Completions data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("C_A", year, context)
    load_to_snowflake(df, "C_A", snowflake, context, year)


@asset(
    name="ipeds_enrollment",
    description="IPEDS 12-Month Enrollment (EFFY) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_effy(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Enrollment data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("EFFY", year, context)
    load_to_snowflake(df, "EFFY", snowflake, context, year)


@asset(
    name="ipeds_graduation_rates",
    description="IPEDS Graduation Rates (GR) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_gr(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Graduation Rates data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("GR", year, context)
    load_to_snowflake(df, "GR", snowflake, context, year)


@asset(
    name="ipeds_financial_aid",
    description="IPEDS Student Financial Aid (SFA) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_sfa(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Financial Aid data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("SFA", year, context)
    load_to_snowflake(df, "SFA", snowflake, context, year)


@asset(
    name="ipeds_retention_rates",
    description="IPEDS Retention Rates (EF_D) - raw data loaded to Snowflake by year",
    group_name="ipeds_ingestion",
    partitions_def=ipeds_years_partitions,
)
def ipeds_ef_d(context: AssetExecutionContext, snowflake: SnowflakeResource) -> None:
    """Download and load IPEDS Retention Rates data for a specific year."""
    year = int(context.partition_key)
    df = download_ipeds_file("EF_D", year, context)
    load_to_snowflake(df, "EF_D", snowflake, context, year)


@dbt_assets(
    manifest=texas_cc_benchmarking_project.manifest_path,
)
def texas_cc_benchmarking_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt transformations on IPEDS data."""
    yield from dbt.cli(["build"], context=context).stream()