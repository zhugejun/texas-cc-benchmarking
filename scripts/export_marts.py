#!/usr/bin/env python3
"""
Export the dbt marts from DuckDB to flat files for Power BI / Excel / Tableau.

Power BI has no DuckDB connector, so the hand-off is a folder of flat files that
any BI tool reads natively. Run this after `dbt build`, then copy the exports/
folder to the machine that has Power BI Desktop.

Usage:
    # All marts to exports/ as CSV (Power BI friendly, UTF-8 with BOM)
    python scripts/export_marts.py

    # Parquet instead (smaller, keeps dtypes; Power BI reads it natively)
    python scripts/export_marts.py --format parquet

    # Both formats, custom destination
    python scripts/export_marts.py --format both --outdir ~/Desktop/tx_cc

    # Just one mart
    python scripts/export_marts.py --tables fct_student_outcomes
"""

import argparse
import os
from pathlib import Path

import duckdb

# The analytics-ready tables. Anything downstream should read these, not the
# staging/intermediate layers.
MARTS = [
    'dim_texas_institutions',
    'fct_student_outcomes',
    'rpt_peer_comparison',
    'rpt_equity_dashboard',
]


def duckdb_path() -> Path:
    """Locate the DuckDB file produced by `dbt build`.

    Override with the DUCKDB_PATH env var; otherwise default to the file in the
    dbt project directory.
    """
    env_path = os.environ.get("DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    repo_root = Path(__file__).resolve().parent.parent
    return repo_root / "texas_cc_benchmarking" / "texas_cc.duckdb"


def export(con: duckdb.DuckDBPyConnection, table: str, outdir: Path, fmt: str) -> None:
    df = con.execute(f"SELECT * FROM {table}").df()

    if fmt in ("csv", "both"):
        dest = outdir / f"{table}.csv"
        # utf-8-sig: the BOM makes Excel and Power BI on Windows detect UTF-8
        # instead of falling back to a local codepage and mangling accented names.
        df.to_csv(dest, index=False, encoding="utf-8-sig")
        print(f"  {dest.name:<32} {len(df):>6} rows  {dest.stat().st_size / 1024:>7.1f} KB")

    if fmt in ("parquet", "both"):
        dest = outdir / f"{table}.parquet"
        # DuckDB writes Parquet natively, so this needs no pyarrow and keeps the
        # column types straight from the warehouse rather than round-tripping
        # through pandas.
        con.execute(
            f"COPY (SELECT * FROM {table}) TO ? (FORMAT PARQUET)", [str(dest)]
        )
        print(f"  {dest.name:<32} {len(df):>6} rows  {dest.stat().st_size / 1024:>7.1f} KB")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export dbt marts from DuckDB to flat files for BI tools.",
    )
    parser.add_argument("--db", type=Path, default=None,
                        help="Path to the DuckDB file (default: texas_cc_benchmarking/texas_cc.duckdb)")
    parser.add_argument("--outdir", type=Path, default=None,
                        help="Destination directory (default: exports/ at the repo root)")
    parser.add_argument("--format", choices=["csv", "parquet", "both"], default="csv",
                        help="Output format (default: csv)")
    parser.add_argument("--tables", nargs="+", default=MARTS,
                        help=f"Tables to export (default: {' '.join(MARTS)})")
    args = parser.parse_args()

    db_path = args.db or duckdb_path()
    if not db_path.exists():
        raise SystemExit(
            f"DuckDB database not found at {db_path}.\n"
            "Run `uv run dbt build` from the texas_cc_benchmarking/ directory first."
        )

    outdir = args.outdir or Path(__file__).resolve().parent.parent / "exports"
    outdir.mkdir(parents=True, exist_ok=True)

    # read_only lets this run while dbt or the dashboard has the file open.
    con = duckdb.connect(str(db_path), read_only=True)

    print(f"Reading {db_path}")
    print(f"Writing {outdir}\n")
    for table in args.tables:
        export(con, table, outdir, args.format)

    print(f"\nDone. Copy {outdir} to the machine running Power BI Desktop.")
    print("See docs/powerbi.md for the import steps.")


if __name__ == "__main__":
    main()
