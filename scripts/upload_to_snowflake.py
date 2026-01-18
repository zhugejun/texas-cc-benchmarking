#!/usr/bin/env python3
"""
Upload IPEDS seed files to Snowflake using COPY command.
Much faster than dbt seed.
"""

import glob
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
    schema=profile['schema'],
    warehouse='COMPUTE_WH'
)

cursor = conn.cursor()

# Create file format first
print('Creating file format...')
cursor.execute("""
    CREATE OR REPLACE FILE FORMAT ipeds_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('', 'NULL')
        ENCODING = 'UTF8'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
""")
print('✓ File format created\n')

# Get all CSV files
seeds_dir = Path(__file__).parent.parent / 'texas_cc_benchmarking' / 'seeds'
csv_files = sorted(seeds_dir.glob('*.csv'))

print(f'Found {len(csv_files)} CSV files to upload\n')

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
                        FILE_FORMAT => 'ipeds_csv_format'
                    )
                )
            )
        """)
        
        # Step 3: Load data
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM '@~/staged/{csv_file.name}'
            FILE_FORMAT = ipeds_csv_format
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
