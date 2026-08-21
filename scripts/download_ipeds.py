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

Progress and problems are logged to stdout and to logs/ipeds_download.log.
A summary of everything that went wrong is logged at the end of the run.
"""

import argparse
import io
import logging
import statistics
import sys
import zipfile
from collections import defaultdict, namedtuple
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
    'EF_D': 'Rental Rates',
}

# IPEDS serves complete data files from two paths: newer years (2023+) live
# under /complete-data-files/, older years remain under /datacenter/data/.
# Try both so any year resolves without a hardcoded cutoff.
BASE_URLS = [
    'https://nces.ed.gov/ipeds/complete-data-files',
    'https://nces.ed.gov/ipeds/datacenter/data',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Columns HD must expose for the Texas community college filter to be correct.
REQUIRED_HD_COLUMNS = ('UNITID', 'STABBR', 'SECTOR')

# A year whose row count falls below this share of the median across the other
# years of the same dataset is flagged as suspect (usually a provisional release).
THIN_FILE_RATIO = 0.5
MIN_YEARS_FOR_THIN_CHECK = 3


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

LEVEL_EMOJI = {
    'DEBUG': '🔍',
    'INFO': 'ℹ️',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'CRITICAL': '🚨',
}

LOG_FORMAT = '%(asctime)s.%(msecs)03d %(emoji)s %(levelname)s %(emoji)s %(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'

logger = logging.getLogger('ipeds')


class EmojiFormatter(logging.Formatter):
    """Wrap the level name in an emoji so warnings and errors stand out."""

    def format(self, record):
        record.emoji = LEVEL_EMOJI.get(record.levelname, '')
        return super().format(record)


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = EmojiFormatter(LOG_FORMAT, datefmt=LOG_DATEFMT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# --------------------------------------------------------------------------
# Run tracking
# --------------------------------------------------------------------------

# status is one of: 'ok', 'empty', 'not_published', 'error'
Result = namedtuple('Result', ['dataset', 'year', 'status', 'reason', 'rows'])

# Every dataset/year attempted, recorded once even when downloaded twice.
_results: dict[tuple[str, int], Result] = {}

# HD is needed both to build the Texas UNITID list and as a dataset in its own
# right, so cache it to avoid downloading each year twice.
_hd_cache: dict[tuple[str, int], tuple] = {}


def record(dataset: str, year: int, status: str, reason: str | None, rows: int) -> None:
    _results[(dataset, year)] = Result(dataset, year, status, reason, rows)


def update_result(dataset: str, year: int, **changes) -> None:
    key = (dataset, year)
    if key in _results:
        _results[key] = _results[key]._replace(**changes)


def get_table_name(dataset: str, year: int) -> str:
    """Build the IPEDS table name (zip basename) for a dataset/year."""
    if dataset == 'C_A':
        # Completions: C2023_A format
        return f'C{year}_A'
    elif dataset == 'SFA':
        # Student Financial Aid: SFA2324 format (academic year)
        prev_year = str(year - 1)[-2:]
        curr_year = str(year)[-2:]
        return f'SFA{prev_year}{curr_year}'
    elif dataset == 'EF_D':
        # Retention Rates: EF2024D format
        return f'EF{year}D'
    else:
        # Standard format: HD2023, EFFY2023, GR2023
        return f'{dataset}{year}'


def read_zip_csv(content: bytes, table_name: str):
    """
    Extract the data CSV from an IPEDS zip.

    Returns:
        (DataFrame, None) on success, or (None, reason) describing the problem.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]

        if not csv_files:
            return None, 'no CSV inside zip'

        # Prefer the base file over the revised (_rv) companion when both ship
        csv_files.sort(key=lambda f: ('_rv' in f.lower(), f.lower()))
        csv_filename = csv_files[0]
        logger.info('Extracting %s', csv_filename)

        # IPEDS mixes UTF-8 and latin-1 across years
        for encoding in ('utf-8-sig', 'latin-1'):
            try:
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, encoding=encoding, low_memory=False)
                    return df, None
            except UnicodeDecodeError:
                continue

    return None, f'could not decode {csv_filename}'


def download_dataset(dataset: str, year: int):
    """
    Download a single IPEDS dataset.

    Returns:
        (DataFrame, status, reason). DataFrame is None unless status is 'ok'.
    """
    key = (dataset, year)
    if key in _hd_cache:
        logger.info('Reusing already-downloaded %s %s', dataset, year)
        return _hd_cache[key]

    table_name = get_table_name(dataset, year)

    def finish(df, status, reason):
        record(dataset, year, status, reason, len(df) if df is not None else 0)
        if dataset == 'HD':
            _hd_cache[key] = (df, status, reason)
        return df, status, reason

    attempts = []  # (http_status_code_or_None, message) per base URL tried
    for base_url in BASE_URLS:
        url = f'{base_url}/{table_name}.zip'
        logger.info('Downloading %s %s: %s', dataset, year, url)

        try:
            response = requests.get(url, headers=HEADERS, timeout=300)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code == 404:
                logger.info('%s %s not at this path (404), trying next', dataset, year)
            else:
                logger.warning('%s %s returned HTTP %s from %s', dataset, year, code, base_url)
            attempts.append((code, f'HTTP {code} from {base_url}'))
            continue
        except requests.exceptions.RequestException as e:
            logger.warning('%s %s network failure on %s: %s', dataset, year, base_url, e)
            attempts.append((None, f'network failure on {base_url}: {e}'))
            continue

        try:
            df, reason = read_zip_csv(response.content, table_name)
        except zipfile.BadZipFile as e:
            reason = f'invalid zip, server likely returned an HTML error page ({e})'
            logger.error('%s %s failed: %s', dataset, year, reason)
            return finish(None, 'error', reason)

        if df is None:
            logger.error('%s %s failed: %s', dataset, year, reason)
            return finish(None, 'error', reason)

        # Add YEAR column
        df['YEAR'] = year

        logger.info(
            '%s %s downloaded: %s rows, %s columns',
            dataset, year, f'{len(df):,}', len(df.columns),
        )
        return finish(df, 'ok', None)

    # Every base URL failed. A 404 everywhere means IPEDS has not released the
    # file yet, which is expected for the current year; anything else is a real
    # problem worth an ERROR.
    if attempts and all(code == 404 for code, _ in attempts):
        reason = 'not published yet (404 on both IPEDS paths)'
        logger.warning('%s %s %s', dataset, year, reason)
        return finish(None, 'not_published', reason)

    reason = '; '.join(message for _, message in attempts)
    logger.error('%s %s failed: %s', dataset, year, reason)
    return finish(None, 'error', reason)


def clean_column_name(col: str) -> str:
    """Clean column name by removing BOM and whitespace."""
    # Remove various BOM characters
    cleaned = col.strip()
    # Try UTF-8 BOM
    if cleaned.startswith('﻿'):
        cleaned = cleaned[1:]
    # Try UTF-8 BOM as bytes (might be encoded differently)
    if cleaned.startswith('ï»¿'):
        cleaned = cleaned[3:]
    # Strip again after BOM removal
    return cleaned.strip().upper()


def find_columns(df: pd.DataFrame, names) -> dict:
    """Map each requested cleaned column name to its actual column in df."""
    found = {}
    for col in df.columns:
        clean_name = clean_column_name(col)
        if clean_name in names:
            found[clean_name] = col
    return found


def filter_texas_cc(df: pd.DataFrame):
    """
    Filter an HD frame to Texas public community colleges.

    SECTOR: 1 = Public 4-year (CCs with bachelor's programs)
            4 = Public 2-year (traditional community colleges)

    Returns:
        (DataFrame, None) on success, or (None, reason) if columns are missing.
    """
    columns = find_columns(df, REQUIRED_HD_COLUMNS)
    missing = [name for name in REQUIRED_HD_COLUMNS if name not in columns]
    if missing:
        return None, f'missing required columns: {", ".join(missing)}'

    texas_cc = df[(df[columns['STABBR']] == 'TX') & (df[columns['SECTOR']].isin([1, 4]))]
    return texas_cc, None


def get_texas_cc_unitids(years: list[int]) -> set:
    """
    Download HD datasets and get UNITIDs for Texas community colleges.

    Args:
        years: List of years to get UNITIDs for

    Returns:
        Set of UNITIDs for Texas community colleges across all years
    """
    logger.info('Getting Texas Community College UNITIDs from HD dataset')

    all_unitids = set()

    for year in years:
        df, status, _ = download_dataset('HD', year)
        if status != 'ok':
            continue

        texas_cc, reason = filter_texas_cc(df)
        if texas_cc is None:
            # A renamed IPEDS column would silently shrink the UNITID list and
            # under-filter every other dataset, so treat it as an error.
            logger.error('HD %s cannot be filtered: %s', year, reason)
            update_result('HD', year, status='error', reason=reason)
            continue

        unitid_col = find_columns(df, REQUIRED_HD_COLUMNS)['UNITID']
        unitids = set(texas_cc[unitid_col].unique())
        all_unitids.update(unitids)
        logger.info('HD %s: found %d Texas public community colleges', year, len(unitids))

    if not all_unitids:
        logger.error('No Texas community college UNITIDs found; downstream filters will be empty')
    else:
        logger.info('Total unique Texas CC UNITIDs: %d', len(all_unitids))

    return all_unitids


def filter_by_unitids(df: pd.DataFrame, unitids: set, dataset: str, year: int) -> pd.DataFrame:
    """
    Filter DataFrame to only include specified UNITIDs.

    Args:
        df: Input DataFrame
        unitids: Set of UNITIDs to keep
        dataset: Dataset name
        year: Year being filtered, for logging

    Returns:
        Filtered DataFrame
    """
    columns = find_columns(df, ('UNITID',))
    if 'UNITID' not in columns:
        logger.warning('%s %s has no UNITID column, cannot filter', dataset, year)
        return df

    original_count = len(df)
    df = df[df[columns['UNITID']].isin(unitids)]
    logger.info(
        '%s %s filtered %s → %s rows',
        dataset, year, f'{original_count:,}', f'{len(df):,}',
    )
    return df


# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------

def find_thin_results(results):
    """
    Find years whose row count is far below the other years of the same dataset.

    Returns:
        List of (Result, median_rows) for each suspect year.
    """
    suspects = []
    by_dataset = defaultdict(list)
    for result in results:
        if result.status == 'ok':
            by_dataset[result.dataset].append(result)

    for rows_per_year in by_dataset.values():
        if len(rows_per_year) < MIN_YEARS_FOR_THIN_CHECK:
            continue
        median = statistics.median(r.rows for r in rows_per_year)
        if median <= 0:
            continue
        for result in rows_per_year:
            if result.rows < median * THIN_FILE_RATIO:
                suspects.append((result, median))

    return suspects


def log_summary() -> None:
    """Log a compact rundown of everything that failed or looks suspect."""
    results = sorted(_results.values(), key=lambda r: (r.dataset, r.year))

    ok = [r for r in results if r.status == 'ok']
    empty = [r for r in results if r.status == 'empty']
    not_published = [r for r in results if r.status == 'not_published']
    errors = [r for r in results if r.status == 'error']
    thin = find_thin_results(results)

    logger.info('-' * 80)
    logger.info(
        'Summary: %d ok, %d not-yet-published, %d suspect, %d errors',
        len(ok), len(not_published), len(thin) + len(empty), len(errors),
    )

    if not_published:
        logger.warning(
            'Not published: %s',
            ', '.join(f'{r.dataset} {r.year}' for r in not_published),
        )

    for result, median in thin:
        logger.warning(
            'Suspect: %s %s (%s rows vs ~%s median for other years)',
            result.dataset, result.year, f'{result.rows:,}', f'{int(median):,}',
        )

    for result in empty:
        logger.warning(
            'Suspect: %s %s downloaded but 0 rows after the Texas filter',
            result.dataset, result.year,
        )

    for result in errors:
        logger.error('%s %s failed: %s', result.dataset, result.year, result.reason)

    if not (not_published or thin or empty or errors):
        logger.info('All datasets downloaded cleanly')


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

    project_root = Path(__file__).parent.parent
    setup_logging(project_root / 'logs' / 'ipeds_download.log')

    # Determine years to download
    if args.years:
        years = sorted(args.years)
    elif args.year:
        years = [args.year]
    else:
        years = [2024]

    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = project_root / 'texas_cc_benchmarking' / 'seeds'

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info('-' * 80)
    logger.info('Downloading IPEDS data for years: %s', ', '.join(map(str, years)))
    logger.info('Output directory: %s', output_dir)
    logger.info('Datasets: %s', ', '.join(args.datasets))
    logger.info('Filter Texas CC: %s', args.filter_texas)

    # Get Texas CC UNITIDs if filtering
    texas_cc_unitids = None
    if args.filter_texas:
        texas_cc_unitids = get_texas_cc_unitids(years)

    # Download each dataset
    for dataset in args.datasets:
        logger.info('%s - %s', dataset, DATASETS[dataset])

        for year in years:
            df, status, _ = download_dataset(dataset, year)
            if status != 'ok':
                continue

            if args.filter_texas and texas_cc_unitids:
                if dataset == 'HD':
                    filtered, reason = filter_texas_cc(df)
                    if filtered is None:
                        logger.error('HD %s cannot be filtered: %s', year, reason)
                        update_result('HD', year, status='error', reason=reason)
                        continue
                    logger.info(
                        'HD %s filtered %s → %s rows',
                        year, f'{len(df):,}', f'{len(filtered):,}',
                    )
                    df = filtered
                else:
                    df = filter_by_unitids(df, texas_cc_unitids, dataset, year)

            # Save each year as a separate file
            output_file = output_dir / f'{dataset.lower()}_{year}.csv'
            df.to_csv(output_file, index=False)
            logger.info('Saved %s rows to %s', f'{len(df):,}', output_file)

            update_result(
                dataset, year,
                rows=len(df),
                status='empty' if len(df) == 0 else 'ok',
            )

    log_summary()
    logger.info('Done')


if __name__ == '__main__':
    main()
