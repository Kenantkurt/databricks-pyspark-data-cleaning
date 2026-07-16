# Databricks PySpark — Data Cleaning Practice

Hands-on **PySpark data cleaning** exercises built on **Databricks**, following a
**Bronze → Silver** (medallion) approach. Each notebook takes a raw, messy dataset
and turns it into a clean, typed, analytics-ready Delta table. The latest project
extends the pattern to a **Gold** layer with finance-ready summary tables.

> These are focused practice projects I built while learning PySpark and Spark
> data engineering on Databricks — each one drills a specific set of cleaning skills
> on a real-world messy dataset.

## Tech stack

- **PySpark** (DataFrame API)
- **Spark SQL** (same logic expressed in SQL where useful)
- **Databricks** (notebooks + Unity Catalog volumes)
- **Delta Lake** (Silver tables saved via `saveAsTable`)

## What "Bronze → Silver" means here

| Layer | What it holds |
|-------|---------------|
| **Bronze** | Raw data, read as-is from the source file |
| **Silver** | Cleaned, validated, correctly typed data — ready for analysis/modeling |

Cleaning (null handling, deduplication, type fixing, removing fake values) happens
in the **Bronze → Silver** step — exactly what these notebooks demonstrate.

## Projects

### 1 · E-commerce orders → Silver
[`notebooks/01-ecommerce-orders-bronze-to-silver.ipynb`](notebooks/01-ecommerce-orders-bronze-to-silver.ipynb)

Clean a raw e-commerce orders CSV.
- **Schema on read** with `StructType` instead of `inferSchema` (correct types, single read)
- Inspect data quality: nulls in `quantity`, `order_date`, `unit_price`
- Derive a `revenue` column (`quantity * unit_price`, rounded)
- Drop rows with a null key (`order_id`) or null `revenue`
- Save Silver as a **Delta** table (`ecommerce_orders_silver`)
- Same exploration shown in both **DataFrame API** and **Spark SQL**

### 2 · Dirty cafe sales → Silver
[`notebooks/02-dirty-cafe-sales-bronze-to-silver.ipynb`](notebooks/02-dirty-cafe-sales-bronze-to-silver.ipynb)

Clean a deliberately messy cafe transactions dataset.
- Standardize column names with `withColumnsRenamed`
- Schema on read with typed columns (`DecimalType`, `DateType`, …)
- Data-quality checks: `count` vs `count(distinct)`, null analysis across key columns
- **Row-keep logic** with `filter(~(...))` (drop only rows missing both `total_spent`
  and the inputs needed to recompute it)
- Recover missing revenue with `coalesce(total_spent, quantity * price_per_unit)`
- Turn fake values (`"ERROR"`, `"UNKNOWN"`) into real `NULL` with `CASE WHEN`
- Build the Silver table with a **CTE** + `CREATE OR REPLACE TABLE` (idempotent)

### 3 · Netflix titles → Silver
[`notebooks/03-netflix-titles-bronze-to-silver.ipynb`](notebooks/03-netflix-titles-bronze-to-silver.ipynb)

Clean the Netflix titles catalog (multi-line, quoted CSV).
- Robust CSV read: `multiLine`, `quote`, `escape` for embedded commas/newlines
- Duplicate checks on `show_id` and `title`
- Normalize text columns with `trim` / `lower` / `regexp_replace`
- Split a mixed `duration` field into `duration_min` and `duration_season`
  with `when().otherwise()`
- Type casting + `to_date("MMMM d, yyyy")` for `date_added`
- `split(...)[0].cast(...)` to extract numeric duration values
- Save Silver as a Delta table (`netflix_silver`)

### 4 · Gym workout sessions → Silver
[`notebooks/04-gym-workout-sessions-bronze-to-silver.ipynb`](notebooks/04-gym-workout-sessions-bronze-to-silver.ipynb)

Clean a messy gym workout-log CSV, with a strong focus on **order of operations**
(clean the text *before* changing the type).
- Read everything as text first (raw Bronze) — let nothing get silently coerced
- Two kinds of duplicates: fully identical rows vs. repeated `session_id`
- Normalize text with `trim` + `initcap` so `groupBy` doesn't split categories
- Turn fake placeholders (`"ERROR"`, `"N/A"`, `"UNKNOWN"`) into real `NULL`
- Clean currency text **before** casting: strip `₺` and convert `149,90` → `149.90`
- Parse a column with **mixed date formats** using `coalesce(try_to_date(...), ...)`
- Derive `calories_per_min` and an `intensity` label with `when().otherwise()`
- Apply business-rule filters (keep `duration_min > 0`, `calories_burned > 0`)
- Save Silver as an idempotent Delta table (`gym_silver`, `overwrite`)

### 5 · E-commerce event logs → Silver
[`notebooks/05-ecommerce-event-logs-bronze-to-silver.ipynb`](notebooks/05-ecommerce-event-logs-bronze-to-silver.ipynb)

Parse a raw, **unstructured `.log` file** (one messy text line per event) into a
clean, typed Silver table — the hardest of the set because the input isn't tabular.
- Read as **plain text** (one line = one string column), not CSV — the `|`
  separators would shred a CSV read
- Tried an **LLM extraction** (`ai_query` with a JSON `responseFormat`) first, then
  switched to **`regexp_extract`** — deterministic, same result every run
  (an honest note on *why* the rule-based approach won here)
- Parse a timestamp column with **two different date formats** using
  `coalesce(try_to_timestamp(...), try_to_timestamp(...))` — no crash on non-matches
- Derive a real `date`, a human-readable label (`date_format`), plus date math
  (`date_add` refund window, `date_diff` recency)
- Turn placeholders (`N/A`, `ERROR`, `UNKNOWN`, `-`) into real `NULL`
- Clean dirty money text (`29.99 USD`, `149,90 TL`, `$14.99`) **before** casting to
  `decimal(10,2)` — strip symbols, fix the comma decimal separator
- Strip junk characters from product names, drop business-invalid negative amounts
  (but keep nulls — missing ≠ invalid)
- Save Silver as an idempotent Delta table (`events_silver`, `overwrite`)

### 6 · IoT device events → Silver
[`notebooks/06-iot-device-events-bronze-to-silver.ipynb`](notebooks/06-iot-device-events-bronze-to-silver.ipynb)

Clean IoT device events with **nested JSON columns** — plus a hard-earned lesson
about table grain.
- Robust CSV read (`quote` / `escape` / `multiline` — the JSON cells contain commas)
- Rename messy headers (`Device ID`, `Temp_reading (C)`) in one `withColumnsRenamed` pass
- **Grain-aware dedup**: the table is event-level, so dedupe **exact full-row
  duplicates only** — `dropDuplicates(["device_id"])` would silently delete real
  events (and a matching row count can hide it)
- Timestamps in **three different formats** → `coalesce(try_to_timestamp × 3)`,
  then verify nothing became `NULL` silently
- Placeholders (`N/A`, `UNKNOWN`, `ERROR`) → real `NULL`; currency text
  (`$`, `€`, comma decimals, `EUR` suffix) cleaned **before** casting to `decimal(10,2)`
- Negative cost → **keep the row, null the value** (the event is real, only its
  cost is broken — a different call than dropping the row)
- **Timezone normalization**: `convert_timezone` CET → UTC at the silver layer
- **Nested JSON in three shapes**, each with a matching schema: fixed keys →
  `struct`, repeated items → `array<struct>`, arbitrary pairs → `map<string,string>`
- Clean a field *inside* a struct with **`withField`** — no flattening needed
- Save Silver as an idempotent Delta table (`iot_device_events_silver`, `overwrite`)

### 7 · UK online retail → Silver → Gold (take-home style)
[`notebooks/07-uk-online-retail-take-home-pipeline.ipynb`](notebooks/07-uk-online-retail-take-home-pipeline.ipynb)

A step up in format: a **real dataset** (~540k actual UK retail transactions) worked
as a **take-home assignment** — no task list, only business requirements (R1–R5),
with every decision defended in a written **decision log**.
- Requirements-driven design: raw layer, clean sales table, finance summaries,
  re-runnability, decision log
- Read everything as **string** (no `inferSchema`), profile before cleaning:
  grain proof (`InvoiceNo + CustomerID` not unique → one row = one invoice line),
  null counts, numeric ranges
- **Business-rule filter**: returns/cancellations (`quantity <= 0`) and adjustments
  (`unit_price <= 0`) are real events but not *sales* → 11,809 rows removed, with counts
- **Null `CustomerID` on 25% of rows — kept**: still real sales; dropping them would
  understate revenue by a quarter
- Full-row `dropDuplicates()` at the invoice-line grain (5,226 double-export artifacts)
- **Gold layer**: monthly revenue per country + top-10 products, each in both
  **Spark SQL and DataFrame API**; service codes (`POST`, `M`, `DOT`) in the top-10
  flagged as non-products
- Idempotent `overwrite` writes + a code-review style **decision log** (R5)

### 9 · Retail orders → Silver (typing & renaming drill)
[`notebooks/09-retail-orders-bronze-to-silver.ipynb`](notebooks/09-retail-orders-bronze-to-silver.ipynb)

A focused review project drilling the two core moves of every Bronze → Silver job:
**fixing messy column names** and **converting an all-text table to real types** —
requirements-only format (R1–R6), with interview-style Q&A answered after every step.
*(Project 8 — a larger e-commerce take-home — is in progress and will land separately.)*
- Messy headers (`Order ID`, `cust_NAME`, `' Product '` with hidden spaces) fixed in one
  `withColumnsRenamed` pass — a rename with a wrong source name **silently does nothing**
- Placeholders (`ERROR`, `UNKNOWN`, `N/A`) counted per column first, then turned into
  real `NULL` with `CASE WHEN` — **before** any casting
- Currency text (`₺`, `TL`) stripped, then cast to `decimal(10,2)`; `%` stripped
  from `discount`
- **Six different date formats** in one column → `coalesce(try_to_date × 6)`;
  before/after null counts prove the cast lost nothing — a missing format first showed
  up as 25 silent nulls and was fixed by adding formats until the count returned to 0
- Normalize `status` / `category` / `product` with `trim` + `initcap` so `groupBy`
  doesn't split one real value into fake buckets
- Business rule: null quantity **kept** (missing), `quantity <= 0` **dropped** (invalid) —
  two different problems, two different decisions
- Final checks (row count, business rule, `order_id` grain) + idempotent Delta write
  (`retail_orders_silver`, `overwrite`)

## Datasets

The raw CSVs are read from Databricks Unity Catalog **Volumes**
(`/Volumes/dev/spark_db/datasets/mini-projects/raw_data/`). Sources:

- E-commerce orders — synthetic practice dataset
- Dirty cafe sales — [Kaggle: "Cafe Sales - Dirty Data for Cleaning Training"](https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training)
- Netflix titles — [Kaggle: "Netflix Movies and TV Shows"](https://www.kaggle.com/datasets/shivamb/netflix-shows)
- Gym workout sessions — synthetic practice dataset
- E-commerce event logs — synthetic practice dataset
- IoT device events — synthetic practice dataset
- UK online retail — [Kaggle: "E-Commerce Data" (real UK retailer transactions)](https://www.kaggle.com/datasets/carrie1/ecommerce-data)
- Retail orders — synthetic practice dataset

## Notes

- Notebooks were authored and run on **Databricks** (cell outputs stripped for clean
  rendering on GitHub).
- `%sql` cells show the same step in SQL alongside the DataFrame API — on purpose,
  to keep both fresh.
- Focus is the **cleaning logic**, not the source data.
