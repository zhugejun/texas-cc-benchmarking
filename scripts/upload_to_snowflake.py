#!/usr/bin/env python3
"""
Upload IPEDS seed files to Snowflake using COPY command.
Much faster than dbt seed.

Usage:
    python upload_to_snowflake.py              # Upload all CSVs
    python upload_to_snowflake.py "ef_d_*"     # Upload only ef_d files
"""

import sys
from pathlib import Path
import snowflake.connector
import yaml

# Read Snowflake credentials from dbt profile
with open(Path.home() / '.dbt' / 'profiles.yml') as f:
    profiles = yaml.safe_load(f)
    
profile = profiles['texas_cc_benchmarking']['outputs']['dev']

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=profile['account'],
    user=profile['user'],
    password=profile['password'],
    role=profile['role'],
    database=profile['database'],
    schema='RAW_IPEDS',
    warehouse='COMPUTE_WH'
)

cursor = conn.cursor()

# Create file formats
print('Creating file formats...')

# Format for INFER_SCHEMA - needs PARSE_HEADER to read column names
cursor.execute("""
    CREATE OR REPLACE FILE FORMAT ipeds_csv_infer
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        PARSE_HEADER = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('', 'NULL')
        ENCODING = 'UTF8'
""")

# Format for COPY INTO - needs SKIP_HEADER to skip header row when loading
cursor.execute("""
    CREATE OR REPLACE FILE FORMAT ipeds_csv_load
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('', 'NULL')
        ENCODING = 'UTF8'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
""")
print('✓ File formats created\n')

# Get CSV files (optionally filtered by pattern)
seeds_dir = Path(__file__).parent.parent / 'texas_cc_benchmarking' / 'seeds'
pattern = sys.argv[1] + '.csv' if len(sys.argv) > 1 else '*.csv'
csv_files = sorted(seeds_dir.glob(pattern))

print(f'Found {len(csv_files)} CSV files matching "{pattern}"\n')

for csv_file in csv_files:
    table_name = csv_file.stem.upper()
    
    print(f'Uploading {csv_file.name} → {table_name}...')
    
    try:
        # Step 1: Upload file to user stage FIRST
        cursor.execute(f"PUT file://{csv_file} @~/staged AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        
        # Step 2: Infer schema and create table
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@~/staged/{csv_file.name}',
                        FILE_FORMAT => 'ipeds_csv_infer'
                    )
                )
            )
        """)
        
        # Step 3: Load data
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM '@~/staged/{csv_file.name}'
            FILE_FORMAT = ipeds_csv_load
            ON_ERROR = 'CONTINUE'
        """)
        
        # Step 4: Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Step 5: Clean up staged file
        cursor.execute(f"REMOVE @~/staged/{csv_file.name}")
        
        print(f'  ✓ Loaded {count:,} rows\n')
        
    except Exception as e:
        print(f'  ✗ Error: {e}\n')

print('Done!')
cursor.close()
conn.close()
