# Building the report in Power BI

Power BI Desktop is **Windows-only** — it does not run on macOS — and it has **no
DuckDB connector**. The ODBC route needs a driver install that managed work
laptops usually block. So the hand-off is a folder of flat files:

```txt
Mac (this repo)                         Windows work machine
────────────────                        ────────────────────
dbt build            ──►  exports/*.csv  ──►  Power BI Desktop
export_marts.py                               (import, model, visualize)
```

## 1. Produce the exports (on this machine)

```bash
cd texas_cc_benchmarking && uv run dbt build && cd ..
uv run python scripts/export_marts.py
```

That writes four files to `exports/`:

| File | Rows | Grain |
| --- | --- | --- |
| `dim_texas_institutions.csv` | 55 | one row per institution |
| `fct_student_outcomes.csv` | 275 | one row per institution-year |
| `rpt_peer_comparison.csv` | 275 | one row per institution-year — see caveat below |
| `rpt_equity_dashboard.csv` | 1,375 | **broken grain — see caveat below** |

Use `--format parquet` if you would rather ship typed columns; Power BI reads
Parquet natively and it sidesteps CSV type-guessing entirely.

## 2. Copy `exports/` to the Windows machine

Any transport works — OneDrive, a USB stick, email. The files are small
(~400 KB total as CSV).

## 3. Import

1. **Home → Get Data → Text/CSV**, pick `dim_texas_institutions.csv`.
2. In the preview, click **Transform Data** rather than Load, and confirm the
   types: `UNITID` should be **Whole Number**, rates **Decimal Number**,
   `institution_name` / `CITY` **Text**. Power BI's type guessing is usually
   right here, but `UNITID` occasionally comes in as text, which breaks the
   relationship in step 4.
3. Repeat for `fct_student_outcomes.csv`.
4. **Close & Apply**.

The CSVs are written UTF-8 with a BOM specifically so Power BI on Windows
detects the encoding instead of falling back to a local codepage.

## 4. Model

Go to **Model view** and create one relationship:

```txt
dim_texas_institutions[UNITID]  1 ──── *  fct_student_outcomes[UNITID]
```

Set cross-filter direction **Single** (dim filters fact). Verified in the source
data: `dim` has 55 unique UNITIDs, `fct` has 275 unique `(UNITID, YEAR)` pairs,
and every fact UNITID exists in the dimension — so this is a clean star schema.

For a proper year slicer, add a small date/year table rather than slicing on
`fct_student_outcomes[YEAR]` directly:

```dax
Years = DISTINCT(fct_student_outcomes[YEAR])
```

then relate `Years[YEAR]` 1 → * `fct_student_outcomes[YEAR]`.

## 5. Measures

These reproduce the four Overview cards from the HTML/Streamlit dashboards:

```dax
Colleges = DISTINCTCOUNT(fct_student_outcomes[UNITID])

Avg Graduation Rate = AVERAGE(fct_student_outcomes[graduation_rate_150])

Avg Retention Rate = AVERAGE(fct_student_outcomes[full_time_retention_rate])

Total Associate Degrees = SUM(fct_student_outcomes[associate_degrees])
```

`AVERAGE` ignores blanks, which matches the `mean()` behaviour in the other two
dashboards — worth knowing, because retention is null for 16 of 55 colleges in
2024, so the average is over the colleges that reported.

For the equity tab:

```dax
Avg Hispanic Gap = AVERAGE(fct_student_outcomes[equity_gap_hispanic])
Avg Black Gap    = AVERAGE(fct_student_outcomes[equity_gap_black])
```

## 6. Visuals

Matching the five tabs of the existing dashboard:

| Tab | Visual | Setup |
| --- | --- | --- |
| Overview | Card ×4 | the four measures above |
| Overview | Table | institution_name + the rate columns, sorted by graduation rate desc |
| Overview | Clustered bar | Y `institution_name`, X `Avg Graduation Rate`, filtered to the latest year |
| Graduation | Line | X `YEAR`, Y `Avg Graduation Rate`, Legend `institution_name` |
| Graduation | Clustered column | X `institution_name`, Y `Avg Graduation Rate`, Legend `YEAR` |
| Retention | Line + column | same as Graduation, swapping in `full_time_retention_rate` |
| Completions | Line + column | `associate_degrees` and `total_completions` |
| Equity | Clustered column | X `institution_name`, Y the three `grad_rate_*` columns |
| Equity | Scatter | X `equity_gap_hispanic`, Y `equity_gap_black`, Details `institution_name` |

Add a **slicer** on `institution_name` (List, multi-select) to replace the
checkbox sidebar. Power BI has no direct equivalent of the "highlight one
college in blue, gray out the rest" behaviour — the closest options are
cross-highlighting from a slicer click, or a DAX measure driving a conditional
color:

```dax
Highlight Color =
IF(
    SELECTEDVALUE(fct_student_outcomes[institution_name]) = SELECTEDVALUE(HighlightPick[institution_name]),
    "#1d4ed8",
    "#cbd5e1"
)
```

applied via **Format → Columns → Color → fx → Field value**, with `HighlightPick`
as a disconnected single-select table of college names.

## 7. Refreshing

Re-run `dbt build` and `export_marts.py`, overwrite the CSVs in the same folder,
then **Home → Refresh** in Power BI. Keep the folder path stable and the refresh
is a single click. If you move the files, fix the path under **Transform Data →
Data source settings**.

---

## Caveats in two of the marts

Found while preparing this guide, and **not yet fixed** — flagging rather than
changing your dbt models unasked.

**`rpt_equity_dashboard` has a cartesian join.** In
[rpt_equity_dashboard.sql](../texas_cc_benchmarking/models/marts/rpt_equity_dashboard.sql)
the final CTE joins on `unitid` alone:

```sql
from institutions i
left join outcomes o on i.unitid = o.unitid       -- 5 rows per institution
left join completions c on i.unitid = c.unitid    -- 5 rows per institution
```

Both `outcomes` and `completions` carry one row per institution-year, so every
graduation year is paired with every completion year: 5 × 5 = **25 rows per
institution**, 1,375 total instead of the correct 275. The values are real but
the pairings are not — a 2020 graduation rate sits on the same row as 2024
completion counts. Any measure over this table is wrong, and there is no `year`
column to filter your way out.

The fix is to join on both keys and expose the year:

```sql
left join outcomes o on i.unitid = o.unitid
left join completions c on o.unitid = c.unitid and o.year = c.year
```

plus `o.year` in the select list.

**`rpt_peer_comparison` omits its year column.** The grain is correct (275 rows
= 55 institutions × 5 years), but no `year` column is selected, so the five rows
per college are indistinguishable. Power BI cannot slice it by year and cannot
form a unique key. Adding `year` to the select list resolves it.

**Until both are fixed, build the Power BI report on `fct_student_outcomes` and
`dim_texas_institutions` only.** Those two are clean, carry the year, and cover
every visual in the table above.
