#!/usr/bin/env python3
"""
Download IPEDS datasets and save to dbt seeds folder.

Usage:
    # Download single year
    python scripts/download_ipeds.py --year 2023
    
    # Download multiple years (combined into one file)
    python scripts/download_ipeds.py --years 2019 2020 2021 2022 2023
    
    # Filter for Texas community colleges only
    python scripts/download_ipeds.py --years 2019 2020 2021 2022 2023 --filter-texas
    
    # Download specific datasets
    python scripts/download_ipeds.py --years 2023 --datasets HD C_A --filter-texas
"""

import argparse
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests


# IPEDS datasets to download
DATASETS = {
    'HD': 'Institutional Characteristics',
    'C_A': 'Completions by Award Level',
    'EFFY': '12-Month Enrollment',
    'GR': 'Graduation Rates',
    'SFA': 'Student Financial Aid',
}

# Base URL for IPEDS data
BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/data'


def download_dataset(dataset: str, year: int) -> pd.DataFrame:
    """
    Download a single IPEDS dataset and return as DataFrame.
    
    Args:
        dataset: Dataset code (e.g., 'HD', 'C_A')
        year: Year to download (e.g., 2023)
        
    Returns:
        DataFrame with the data, or None if download fails
    """
    # Construct filename based on dataset type
    if dataset == 'C_A':
        # Completions: C2023_A format
        table_name = f'C{year}_A'
    elif dataset == 'SFA':
        # Student Financial Aid: SFA2324 format (academic year)
        prev_year = str(year - 1)[-2:]
        curr_year = str(year)[-2:]
        table_name = f'SFA{prev_year}{curr_year}'
    else:
        # Standard format: HD2023, EFFY2023, GR2023
        table_name = f'{dataset}{year}'
    
    # For 2023+, use the new data-generator API
    if year >= 2023:
        import time
        timestamp = str(int(time.time() * 1000))
        url = f'https://nces.ed.gov/ipeds/data-generator?year={year}&tableName={table_name}&HasRV=0&type=csv&t={timestamp}'
        print(f'  Downloading {year}: {url}')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=300)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    print(f'    [ERROR] No CSV file found in download')
                    return None
                
                csv_filename = csv_files[0]
                print(f'    [INFO] Extracting {csv_filename}...')
                
                # Read CSV into DataFrame
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
            
            # Add YEAR column
            df['YEAR'] = year
            
            print(f'    [SUCCESS] Loaded {len(df):,} rows, {len(df.columns)} columns')
            return df
            
        except requests.exceptions.RequestException as e:
            print(f'    [ERROR] Failed to download: {e}')
            return None
        except zipfile.BadZipFile as e:
            print(f'    [ERROR] Invalid zip file: {e}')
            return None
    
    # For pre-2023, use the old ZIP format
    else:
        url = f'{BASE_URL}/{table_name}.zip'
        print(f'  Downloading {year}: {url}')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            # Download the zip file
            response = requests.get(url, headers=headers, timeout=300)
            response.raise_for_status()
            
            # Extract CSV from zip
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    print(f'    [ERROR] No CSV file found in {table_name}.zip')
                    return None
                
                csv_filename = csv_files[0]
                print(f'    [INFO] Extracting {csv_filename}...')
                
                # Read CSV into DataFrame
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, encoding='latin-1', low_memory=False)
                
                # Add YEAR column
                df['YEAR'] = year
                
                print(f'    [SUCCESS] Loaded {len(df):,} rows, {len(df.columns)} columns')
                return df
                
        except requests.exceptions.RequestException as e:
            print(f'    [ERROR] Failed to download: {e}')
            return None
        except zipfile.BadZipFile as e:
            print(f'    [ERROR] Invalid zip file: {e}')
            return None


def clean_column_name(col: str) -> str:
    """Clean column name by removing BOM and whitespace."""
    # Remove various BOM characters
    cleaned = col.strip()
    # Try UTF-8 BOM
    if cleaned.startswith('\ufeff'):
        cleaned = cleaned[1:]
    # Try UTF-8 BOM as bytes (might be encoded differently)
    if cleaned.startswith('ï»¿'):
        cleaned = cleaned[3:]
    # Strip again after BOM removal
    return cleaned.strip().upper()


def get_texas_cc_unitids(years: list[int]) -> set:
    """
    Download HD datasets and get UNITIDs for Texas community colleges.
    
    Args:
        years: List of years to get UNITIDs for
        
    Returns:
        Set of UNITIDs for Texas community colleges across all years
    """
    print('Getting Texas Community College UNITIDs from HD dataset...')
    
    all_unitids = set()
    
    for year in years:
        df = download_dataset('HD', year)
        if df is not None:
            # Find the correct column names using cleaned names
            unitid_col = None
            stabbr_col = None
            sector_col = None
            
            for col in df.columns:
                clean_name = clean_column_name(col)
                if clean_name == 'UNITID':
                    unitid_col = col
                elif clean_name == 'STABBR':
                    stabbr_col = col
                elif clean_name == 'SECTOR':
                    sector_col = col
            
            if not all([unitid_col, stabbr_col, sector_col]):
                print(f'  Year {year}: Missing required columns')
                print(f'    Found: UNITID={unitid_col is not None}, STABBR={stabbr_col is not None}, SECTOR={sector_col is not None}')
                print(f'    All columns: {[clean_column_name(c) for c in df.columns[:20]]}...')
                continue
            
            # Filter for Texas public community colleges
            # SECTOR: 1 = Public 4-year (CCs with bachelor's programs)
            #         4 = Public 2-year (traditional community colleges)
            texas_cc = df[(df[stabbr_col] == 'TX') & (df[sector_col].isin([1, 4]))]
            unitids = set(texas_cc[unitid_col].unique())
            all_unitids.update(unitids)
            print(f'  Year {year}: Found {len(unitids)} Texas public community colleges')
    
    print(f'  Total unique Texas CC UNITIDs: {len(all_unitids)}\n')
    return all_unitids


def filter_by_unitids(df: pd.DataFrame, unitids: set, dataset: str) -> pd.DataFrame:
    """
    Filter DataFrame to only include specified UNITIDs.
    
    Args:
        df: Input DataFrame
        unitids: Set of UNITIDs to keep
        dataset: Dataset name
        
    Returns:
        Filtered DataFrame
    """
    # Find UNITID column using cleaned names
    unitid_col = None
    for col in df.columns:
        if clean_column_name(col) == 'UNITID':
            unitid_col = col
            break
    
    if unitid_col is None:
        print(f'    [WARNING] No UNITID column in {dataset}, cannot filter')
        return df
    
    original_count = len(df)
    df = df[df[unitid_col].isin(unitids)]
    filtered_count = len(df)
    
    print(f'    [INFO] Filtered {original_count:,} → {filtered_count:,} rows')
    return df


def main():
    parser = argparse.ArgumentParser(description='Download IPEDS datasets for dbt seeds')
    parser.add_argument(
        '--year',
        type=int,
        help='Single year to download (deprecated, use --years)'
    )
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        help='Years to download (e.g., 2019 2020 2021)'
    )
    parser.add_argument(
        '--datasets',
        nargs='+',
        default=list(DATASETS.keys()),
        choices=list(DATASETS.keys()),
        help='Datasets to download (default: all)'
    )
    parser.add_argument(
        '--filter-texas',
        action='store_true',
        help='Filter to only Texas community colleges'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Output directory (default: texas_cc_benchmarking/seeds/)'
    )
    
    args = parser.parse_args()
    
    # Determine years to download
    if args.years:
        years = sorted(args.years)
    elif args.year:
        years = [args.year]
    else:
        years = [2023]  # Default to 2023
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        script_dir = Path(__file__).parent
        output_dir = script_dir.parent / 'texas_cc_benchmarking' / 'seeds'
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print('-' * 80)
    print(f'Downloading IPEDS data for years: {", ".join(map(str, years))}')
    print(f'Output directory: {output_dir}')
    print(f'Datasets: {", ".join(args.datasets)}')
    print(f'Filter Texas CC: {args.filter_texas}')
    print()
    
    # Get Texas CC UNITIDs if filtering
    texas_cc_unitids = None
    if args.filter_texas:
        texas_cc_unitids = get_texas_cc_unitids(years)
    
    # Download each dataset
    for dataset in args.datasets:
        print(f'{dataset} - {DATASETS[dataset]}')
        
        # Download each year separately
        for year in years:
            # Skip HD if we already downloaded it for filtering
            if dataset == 'HD' and args.filter_texas and texas_cc_unitids:
                # Re-download HD to get full data (not just for UNITIDs)
                pass
            
            df = download_dataset(dataset, year)
            if df is not None:
                # Filter if requested
                if args.filter_texas and texas_cc_unitids:
                    if dataset == 'HD':
                        # For HD, filter by state and sector directly
                        stabbr_col = None
                        sector_col = None
                        for col in df.columns:
                            clean_name = clean_column_name(col)
                            if clean_name == 'STABBR':
                                stabbr_col = col
                            elif clean_name == 'SECTOR':
                                sector_col = col
                        
                        if stabbr_col and sector_col:
                            original_count = len(df)
                            # Include Texas public community colleges (2-year and 4-year)
                            df = df[(df[stabbr_col] == 'TX') & (df[sector_col].isin([1, 4]))]
                            print(f'    [INFO] Filtered {original_count:,} → {len(df):,} rows')
                        else:
                            print(f'    [WARNING] Cannot filter HD - missing STABBR or SECTOR column')
                    else:
                        # For other datasets, filter by UNITID
                        df = filter_by_unitids(df, texas_cc_unitids, dataset)
                
                # Save each year as a separate file
                output_file = output_dir / f'{dataset.lower()}_{year}.csv'
                df.to_csv(output_file, index=False)
                print(f'  [SUCCESS] Saved {len(df):,} rows to {output_file}')
        
        print()
    
    print('-' * 80)
    print('Done!')
    print(f'\nNext step: cd texas_cc_benchmarking && dbt seed')


if __name__ == '__main__':
    main()
