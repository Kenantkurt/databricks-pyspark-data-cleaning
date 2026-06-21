# Databricks PySpark — Data Cleaning Practice

Hands-on **PySpark data cleaning** exercises built on **Databricks**, following a
**Bronze → Silver** (medallion) approach. Each notebook takes a raw, messy dataset
and turns it into a clean, typed, analytics-ready Delta table.

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

## Datasets

The raw CSVs are read from Databricks Unity Catalog **Volumes**
(`/Volumes/dev/spark_db/datasets/mini-projects/raw_data/`). Sources:

- E-commerce orders — synthetic practice dataset
- Dirty cafe sales — [Kaggle: "Cafe Sales - Dirty Data for Cleaning Training"](https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training)
- Netflix titles — [Kaggle: "Netflix Movies and TV Shows"](https://www.kaggle.com/datasets/shivamb/netflix-shows)

## Notes

- Notebooks were authored and run on **Databricks** (cell outputs stripped for clean
  rendering on GitHub).
- `%sql` cells show the same step in SQL alongside the DataFrame API — on purpose,
  to keep both fresh.
- Focus is the **cleaning logic**, not the source data.
