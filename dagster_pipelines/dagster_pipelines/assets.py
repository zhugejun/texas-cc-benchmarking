import io
import os
import zipfile
from pathlib import Path
from typing import Dict

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset, StaticPartitionsDefinition
from dagster_dbt import DbtCliResource, dbt_assets
from pydantic import Field

from .project import texas_cc_benchmarking_project
from .resources import SnowflakeResource


# Define year partitions for IPEDS data (2019-2024)
ipeds_years_partitions = StaticPartitionsDefinition(
    ["2019", "2020", "2021", "2022", "2023", "2024"]
)


# IPEDS dataset configurations
IPEDS_DATASETS = {
    "hd": {
        "name": "HD",
        "description": "Institutional Characteristics",
        "table": "institutional_characteristics",
    },
    "c_a": {
        "name": "C_A", 
        "description": "Completions by Award Level",
        "table": "completions",
    },
    "effy": {
        "name": "EFFY",
        "description": "12-Month Enrollment",
        "table": "enrollment",
    },
    "gr": {
        "name": "GR",
        "description": "Graduation Rates",
        "table": "graduation_rates",
    },
    "sfa": {
        "name": "SFA",
        "description": "Student Financial Aid",
        "table": "financial_aid",
    },
}


def download_ipeds_file(
    dataset_code: str, 
    year: int, 
    context: AssetExecutionContext,
    full_filename: str = None
) -> pd.DataFrame:
    """
    Download and extract an IPEDS dataset.
    
    Args:
        dataset_code: The IPEDS dataset code (e.g., 'HD', 'EFFY')
        year: The year to download (e.g., 2023)
        context: Asset execution context for logging
        full_filename: Optional full filename (without .zip) for special cases like 'C2023_A'
        
    Returns:
        DataFrame containing the IPEDS data
    """
    # Construct download URL
    if full_filename:
        filename = full_filename
    else:
        filename = f"{dataset_code}{year}"
    
    url = f"https://nces.ed.gov/ipeds/datacenter/data/{filename}.zip"
    
    context.log.info(f"Downloading {filename} from {url}")
    
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Find the CSV file (usually the first .csv file)
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                raise ValueError(f"No CSV file found in {filename}.zip")
            
            csv_filename = csv_files[0]
            context.log.info(f"Extracting {csv_filename} from ZIP archive")
            
            with z.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file, encoding='latin-1', low_memory=False)
                
        context.log.info(f"Successfully downloaded {filename}: {len(df)} rows, {len(df.columns)} columns")
        return df
        
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to download {filename}: {str(e)}")
        raise
    except zipfile.BadZipFile as e:
        context.log.error(f"Invalid ZIP file for {filename}: {str(e)}")
        raise


def load_to_snowflake(
    df: pd.DataFrame,
    table_name: str,
    snowflake: SnowflakeResource,
    context: AssetExecutionContext,
    year: int = None,
) -> None:
    """
    Load a DataFrame to Snowflake into pre-existing tables.
    
    Args:
        df: DataFrame to load
        table_name: Target table name (must already exist)
        snowflake: Snowflake resource
        context: Asset execution context for logging
        year: Year of data being loaded (for partition tracking)
    """
    context.log.info(f"Loading {len(df)} rows to Snowflake table {table_name}" + 
                    (f" for year {year}" if year else ""))
    
    conn = snowflake.get_connection()
    cursor = conn.cursor()
    
    try:
        # Convert column names to uppercase to match Snowflake table schema
        df.columns = [col.upper().replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Add metadata column if not present
        if '_SOURCE_FILE' not in df.columns:
            source_file = f"{table_name}{year}.csv" if year else f"{table_name}.csv"
            df['_SOURCE_FILE'] = source_file
        
        context.log.info(f"Table {table_name} ready (using existing schema)")
        
        # Delete existing data for this year if year is provided (upsert logic)
        if year:
            try:
                # Try to delete by YEAR column if it exists
                delete_sql = f"DELETE FROM {table_name} WHERE YEAR = '{year}'"
                cursor.execute(delete_sql)
                deleted_count = cursor.rowcount
                context.log.info(f"Deleted {deleted_count} existing rows for year {year}")
            except Exception as e:
                context.log.warning(f"Could not delete by YEAR (column may not exist): {e}")
                context.log.info("Proceeding with insert - data will be appended")
        
        # Write DataFrame to temporary CSV
        temp_csv = Path(f"/tmp/{table_name}_{year if year else 'data'}.csv")
        df.to_csv(temp_csv, index=False, encoding='utf-8')
        
        # Load CSV to Snowflake using COPY INTO
        put_sql = f"PUT file://{temp_csv} @%{table_name}"
        cursor.execute(put_sql)
        
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @%{table_name}
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'UTF8'
        )
        ON_ERROR = 'CONTINUE'
        """
        result = cursor.execute(copy_sql)
        
        # Clean up temp file and stage
        temp_csv.unlink()
        cursor.execute(f"REMOVE @%{table_name}")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        context.log.info(f"Successfully loaded data to {table_name}. Total rows in table: {row_count}")
        
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
    # For completions, the file format is C{YEAR}_A (e.g., C2023_A.zip)
    df = download_ipeds_file("C", year, context, full_filename=f"C{year}_A") 
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


@dbt_assets(
    manifest=texas_cc_benchmarking_project.manifest_path,
)
def texas_cc_benchmarking_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt transformations on IPEDS data."""
    yield from dbt.cli(["build"], context=context).stream()