#!/usr/bin/env python3
"""
Build a standalone HTML dashboard from the local DuckDB marts.

The marts are small (55 colleges x 5 years), so the whole dataset is embedded in
the page as JSON and every filter, tab, and chart runs client-side. The result is
one self-contained file: no Python, no server, works offline, and can be emailed
or dropped on SharePoint as-is.

Usage:
    # Fully self-contained (~5 MB, plotly.js inlined) -> dashboard/dist/
    python dashboard/build_html.py

    # Small file (~120 KB) that pulls plotly.js from the CDN; needs internet
    python dashboard/build_html.py --cdn

    # Custom destination
    python dashboard/build_html.py --out ~/Desktop/tx_cc_dashboard.html
"""

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path

import duckdb
import pandas as pd
import plotly

# Years shown in the dashboard. IPEDS releases completions (C_A) about a year
# ahead of graduation rates (GR) and retention (EF_D), so the warehouse can hold
# a newer year than the dashboard should display: showing it would leave the
# graduation, retention, and equity tabs blank for that year. The window stops at
# the newest year where every headline metric exists. Bump both ends once
# GR<yyyy> and EF<yyyy>D are published and loaded.
START_YEAR = 2020
END_YEAR = 2024

# SQL column -> the short key used in the embedded JSON. Only the fields the
# dashboard actually charts are shipped, which keeps the payload small.
COLUMNS = {
    "institution_name": "college",
    "YEAR": "year",
    "graduation_rate_150": "grad_rate",
    "success_rate": "success_rate",
    "full_time_retention_rate": "retention",
    "associate_degrees": "assoc_degrees",
    "total_completions": "total_completions",
    "grad_rate_hispanic": "grad_hispanic",
    "grad_rate_black": "grad_black",
    "grad_rate_white": "grad_white",
    "equity_gap_hispanic": "gap_hispanic",
    "equity_gap_black": "gap_black",
}


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


def plotly_js_path() -> Path:
    return Path(plotly.__file__).resolve().parent / "package_data" / "plotly.min.js"


def plotly_js_version(source: str) -> str | None:
    """Pull the plotly.js version out of the bundle so the CDN pin matches it."""
    match = re.search(r'version\s*[:=]\s*["\']([0-9]+\.[0-9]+\.[0-9]+)["\']', source[:4000])
    return match.group(1) if match else None


def load_rows(db_path: Path) -> list[dict]:
    """Read fct_student_outcomes into JSON-safe records."""
    # read_only lets this run while dbt or the Streamlit app has the file open.
    con = duckdb.connect(str(db_path), read_only=True)
    select = ", ".join(f'"{c}"' for c in COLUMNS)
    df = con.execute(
        f"SELECT {select} FROM fct_student_outcomes "
        'WHERE "YEAR" BETWEEN ? AND ? '
        'ORDER BY "YEAR", institution_name',
        [START_YEAR, END_YEAR],
    ).df()
    con.close()

    df = df.rename(columns=COLUMNS)
    # Round the floats: two decimals is well past what the charts and tables show,
    # and it trims the embedded payload noticeably.
    for col in df.columns:
        if col not in ("college", "year") and pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].round(2)
    df["year"] = df["year"].astype(int)

    # NaN is not valid JSON; turn it into null so the JS null checks work.
    records = df.astype(object).where(pd.notna(df), None).to_dict("records")
    return records


def embed_json(payload: dict) -> str:
    """Serialize for inclusion in a <script> block.

    `</script>` inside a string literal would close the block early, so the
    forward slash is escaped. `\\u2028`/`\\u2029` are legal in JSON but are line
    terminators in older JS parsers.
    """
    text = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    return text.replace("</", "<\\/").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


def build(db_path: Path, out_path: Path, use_cdn: bool) -> None:
    template_path = Path(__file__).resolve().parent / "template.html"
    template = template_path.read_text(encoding="utf-8")

    rows = load_rows(db_path)
    if not rows:
        raise SystemExit(
            f"fct_student_outcomes in {db_path} has no rows for "
            f"{START_YEAR}-{END_YEAR}. Run `uv run dbt build` first."
        )

    years = sorted({r["year"] for r in rows})
    colleges = sorted({r["college"] for r in rows})
    payload = {
        "rows": rows,
        "years": years,
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    js_source = plotly_js_path().read_text(encoding="utf-8")
    if use_cdn:
        version = plotly_js_version(js_source)
        if not version:
            raise SystemExit(
                "Could not detect the bundled plotly.js version, so the CDN URL "
                "cannot be pinned. Re-run without --cdn to inline it instead."
            )
        plotly_block = f'</script>\n<script src="https://cdn.plot.ly/plotly-{version}.min.js">'
    else:
        plotly_block = js_source

    html = template.replace("/*__PLOTLY_JS__*/", plotly_block)
    html = html.replace("/*__DATA__*/", embed_json(payload))

    # Both placeholders must be gone, or the page silently ships without data.
    for placeholder in ("/*__PLOTLY_JS__*/", "/*__DATA__*/"):
        if placeholder in html:
            raise SystemExit(f"Template placeholder {placeholder} was not substituted.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")

    size_mb = out_path.stat().st_size / 1_000_000
    print(f"Wrote {out_path}")
    print(f"  {len(rows)} rows, {len(colleges)} colleges, years {years[0]}-{years[-1]}")
    print(f"  {size_mb:.2f} MB ({'plotly.js from CDN' if use_cdn else 'plotly.js inlined, works offline'})")
    if use_cdn:
        print("  Note: this file needs internet access to render its charts.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a standalone HTML dashboard from the DuckDB marts.",
    )
    parser.add_argument("--db", type=Path, default=None,
                        help="Path to the DuckDB file (default: texas_cc_benchmarking/texas_cc.duckdb)")
    parser.add_argument("--out", type=Path, default=None,
                        help="Output HTML file (default: dashboard/dist/texas_cc_dashboard.html)")
    parser.add_argument("--cdn", action="store_true",
                        help="Link plotly.js from the CDN instead of inlining it (small file, needs internet)")
    args = parser.parse_args()

    db_path = args.db or duckdb_path()
    if not db_path.exists():
        raise SystemExit(
            f"DuckDB database not found at {db_path}.\n"
            "Run `uv run dbt build` from the texas_cc_benchmarking/ directory first."
        )

    out_path = args.out or Path(__file__).resolve().parent / "dist" / "texas_cc_dashboard.html"
    build(db_path, out_path, args.cdn)


if __name__ == "__main__":
    main()
