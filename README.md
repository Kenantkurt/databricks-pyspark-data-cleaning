# Databricks PySpark — Data Cleaning Practice

Hands-on **PySpark data cleaning** exercises built on **Databricks**, following a
**Bronze → Silver** (medallion) approach. Each notebook takes a raw, messy dataset
and turns it into a clean, typed, analytics-ready Delta table. The later projects
extend the pattern to a **Gold** layer with business-ready summary tables — and the
newest one is a pure Gold-layer drill (window functions in SQL and the DataFrame API)
with the core logic extracted into a module and covered by **pytest unit tests**.

> These are focused practice projects I built while learning PySpark and Spark
> data engineering on Databricks — each one drills a specific set of cleaning skills
> on a real-world messy dataset.

## Tech stack

- **PySpark** (DataFrame API)
- **Spark SQL** (same logic expressed in SQL where useful)
- **Databricks** (notebooks + Unity Catalog volumes)
- **Delta Lake** (Silver tables saved via `saveAsTable`)
- **pytest** (unit tests for extracted transformation logic — from project 13 on)

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

### 10 · OranjeCart online shop → Silver → Gold (two-table take-home)
[`notebooks/10-online-shop-two-table-take-home-pipeline.ipynb`](notebooks/10-online-shop-two-table-take-home-pipeline.ipynb)

First project with **two related dirty tables** (customers + orders) that have to be cleaned
separately and then joined — requirements-only take-home format, with a reconciliation
("trust check") and a full decision log.
- Customers: normalize **then** re-deduplicate — case/whitespace near-duplicates survive a
  full-row dedup until the text is normalized (57 → 54 → 52, grain proven with counts)
- Orders: currency text (`€`, `EUR`, comma decimals) stripped before casting; placeholder
  scan across all 7 columns first
- **Recovery over deletion**: 5 corrupted quantities, 3 null prices and 5 missing totals all
  rebuilt from the other two columns (`total ÷ price` gave clean integers) — applied only to
  the broken rows, so the trust check keeps its evidence
- **Trust check**: 12 of 132 rows fail `total = qty × price`, all at ratio ≈ 0.90, all
  Delivered → a 10% discount pattern, not data error; revenue based on `total_amount`
  (money actually collected)
- **Anti joins both directions**: 9 orphan orders (kept in totals, out of breakdowns) vs
  8 customers who never ordered (shown with zeros in Gold)
- **Gold**: month × city revenue (`yyyy-MM` so 2024/2025 don't merge), top-10 customers, and
  a 2025 all-customers summary — orders filtered **before** the left join so zero-order
  customers survive; join fan-out disproven with a row-count check (132 → 132)


### 12 · VeloShop refund requests → Silver → Gold (string repair take-home)
[`notebooks/12-ecommerce-refund-requests-take-home-pipeline.ipynb`](notebooks/12-ecommerce-refund-requests-take-home-pipeline.ipynb)

Single-table take-home built around **string repair**: restoring identifiers that a tool
(Excel) silently corrupted, plus the core cleaning routine — with every removal measured
before it happens.
- **SKU repair with `lpad`**: Excel stripped the leading zeros from 6-digit SKUs — restored
  with `trim` + `lpad(sku, 6, "0")`, justified by the catalog team's rule (not an assumption),
  verified with a `length ≠ 6 → 0 rows` check
- **`split` + `getItem`** on a pipe-separated category tree (escaped `\\|` — `split` takes a
  regex), then the column **renamed** so its name matches its new content
- Duplicate ids proven to be **exact full-row copies** (groupBy all columns) before a
  full-row dedup (108 → 105)
- Four date formats in one column → `coalesce(try_to_date × 4)`, 0 nulls after parse
- Cast safety proven with **before/after null counts** (5 = 5)
- **Negative amounts**: 2 rows, both approved + electronics — measured, inspected, removed
  from Silver with the trade-off logged (they also disappear from the Q3 reason counts) and
  flagged as an upstream data-quality signal
- **Gold**: approved refund cost per category (ranking depends on the negatives decision),
  worst month keyed on `yyyy-MM`, top refund reasons with placeholder junk excluded


### 13 · SnelBite food delivery — Gold layer & window functions (+ first unit tests)
[`notebooks/13-food-delivery-gold-layer-pipeline.ipynb`](notebooks/13-food-delivery-gold-layer-pipeline.ipynb)
· [`gold_functions.py`](notebooks/gold_functions.py) · [`test_gold_functions.py`](notebooks/test_gold_functions.py)

A deliberate change of pace: the data arrives **already clean** (silver), and the whole
exercise is the **Gold layer** — 10 business questions answered in both **SQL and the
DataFrame API**, window-function heavy by design.
- **Month-over-month change per city**: aggregate to city × month first, then
  `lag() over (partition by city order by month)` — biggest drop isolated with the
  first-month nulls sorted last (a `lag` null means "no previous month", not an error)
- **Running total**: `sum() over (order by month rows between unbounded preceding and
  current row)` to find the month cumulative revenue passed €15,000 — and why
  `partition by month` would silently reset the counter every month
- **Order vs city average**: `avg() over (partition by city)` **without** an `order by` —
  the same window with one keeps the grain but turns the value into a running average;
  plus the defense answer for "why can't a plain groupBy do this?"
- **Gold table** `gold_city_monthly` written with an **idempotent overwrite** — which paid
  off immediately when the spec (avg delivery minutes) required a rebuild
- **First unit-tested project**: the top-N-per-city logic extracted into
  `gold_functions.py` and tested with 7 hand-written rows — a deliberate revenue tie
  (why `row_number()` + tie-breaker beats `rank()`) and a group smaller than n; expected
  output written by hand, `assert retcode == 0` as the pipeline gate

### 14 · TulpStay hotel bookings — cleaning + Gold + a unit test that caught a real bug
[`notebooks/14-hotel-bookings-cleaning-gold-pipeline.ipynb`](notebooks/14-hotel-bookings-cleaning-gold-pipeline.ipynb)
· [`hotel_functions.py`](notebooks/hotel_functions.py) · [`test_hotel_functions.py`](notebooks/test_hotel_functions.py)

Back to the full core routine (dirty booking export → silver → gold), with the testing
habit from project 13 carried forward.
- **Measure before you delete**: 143 raw rows → 140 after removing 3 exact duplicates
  (proven with `groupBy(booking_id).count()`), → 138 after dropping 2 impossible
  negative-nights rows; placeholder-born nulls **kept** as an upstream DQ signal
- **Currency repair before the cast**: `€ / EUR / decimal comma` stripped with
  `replace` + `trim`, then `cast(decimal(10,2))` — order matters, because with ANSI mode
  on a dirty string doesn't become a quiet null, it throws `CAST_INVALID_INPUT`
- **`guest_full_name` built the right way round**: trim/initcap each messy column first,
  `concat_ws(" ", ...)` last — clean at the lowest grain, then combine
- **Three date formats, one column**: `coalesce(try_to_date × 3)` → 0 unparsed dates (checked)
- **3-month moving average** with an explicit frame (`rows between 2 preceding and
  current row`) — and the defense answer for why `order by` alone silently gives a
  *running* average instead
- **Best city per month**: aggregate to city × month first, then
  `rank() over (partition by month order by revenue desc)` — the mirror image of
  project 13's "top restaurants per city"
- **The red test earned its keep**: the first version of `clean_price` had no currency
  cleaning; three hand-written rows exposed it in under a second
  (`NumberFormatException: [CAST_INVALID_INPUT] '€ 320.50' ... cannot be cast to
  "DOUBLE"`). The fix went into the function — the hand-written expectation was
  already right.

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
- OranjeCart online shop (customers + orders) — synthetic practice dataset, two related tables
- VeloShop refund requests — synthetic practice dataset
- SnelBite food delivery — synthetic practice dataset (clean by design — Gold-layer drill)
- TulpStay hotel bookings — synthetic practice dataset

## Notes

- Notebooks were authored and run on **Databricks** (cell outputs stripped for clean
  rendering on GitHub).
- `%sql` cells show the same step in SQL alongside the DataFrame API — on purpose,
  to keep both fresh.
- Focus is the **cleaning logic**, not the source data.
