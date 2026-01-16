#!/usr/bin/env python3
"""
Download IPEDS datasets and save to dbt seeds folder.

Usage:
    python scripts/download_ipeds.py --year 2023
    python scripts/download_ipeds.py --year 2023 --datasets HD C_A
"""

import argparse
import io
import zipfile
from pathlib import Path

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


def download_dataset(dataset: str, year: int, output_dir: Path) -> None:
    """
    Download a single IPEDS dataset.
    
    Args:
        dataset: Dataset code (e.g., 'HD', 'C_A')
        year: Year to download (e.g., 2023)
        output_dir: Directory to save CSV files
    """
    # Construct filename based on dataset type
    if dataset == 'C_A':
        # Completions: C2023_A format
        filename = f'C{year}_A'
    elif dataset == 'SFA':
        # Student Financial Aid: SFA2324 format (academic year)
        # For year 2024, use SFA2324 (2023-24 academic year)
        prev_year = str(year - 1)[-2:]
        curr_year = str(year)[-2:]
        filename = f'SFA{prev_year}{curr_year}'
    else:
        # Standard format: HD2023, EFFY2023, GR2023
        filename = f'{dataset}{year}'
    
    url = f'{BASE_URL}/{filename}.zip'

    
    print(f'Downloading {dataset} ({DATASETS[dataset]}) for {year}...')
    print(f'  URL: {url}')
    
    try:
        # Download the zip file
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Extract CSV from zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                print(f'  [ERROR] No CSV file found in {filename}.zip')
                return
            
            csv_filename = csv_files[0]
            print(f'  [INFO] Extracting {csv_filename}...')
            
            # Save to seeds folder with lowercase name
            output_file = output_dir / f'{dataset.lower()}.csv'
            with open(output_file, 'wb') as f:
                f.write(z.read(csv_filename))
            
            print(f'  [SUCCESS] Saved to {output_file}')
            
    except requests.exceptions.RequestException as e:
        print(f'  [ERROR] Failed to download: {e}')
    except zipfile.BadZipFile as e:
        print(f'  [ERROR] Invalid zip file: {e}')


def main():
    parser = argparse.ArgumentParser(description='Download IPEDS datasets for dbt seeds')
    parser.add_argument(
        '--year',
        type=int,
        default=2023,
        help='Year to download (default: 2023)'
    )
    parser.add_argument(
        '--datasets',
        nargs='+',
        default=list(DATASETS.keys()),
        choices=list(DATASETS.keys()),
        help='Datasets to download (default: all)'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Output directory (default: texas_cc_benchmarking/seeds/)'
    )
    
    args = parser.parse_args()
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Default to seeds folder in dbt project
        script_dir = Path(__file__).parent
        output_dir = script_dir.parent / 'texas_cc_benchmarking' / 'seeds'
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print('-' * 80)
    print(f'Downloading IPEDS data for {args.year}')
    print(f'Output directory: {output_dir}')
    print(f'Datasets: {", ".join(args.datasets)}\n')
    
    # Download each dataset
    for dataset in args.datasets:
        download_dataset(dataset, args.year, output_dir)
        print()  # Blank line between datasets
    
    print('-' * 80)
    print('Done!')


if __name__ == '__main__':
    main()
