# 🏭 FMCG Data Engineering Pipeline — Databricks & Delta Lake

> End-to-end data pipeline built on **Databricks**, **Apache Spark**, and **Delta Lake** — ingesting raw CSV data from an S3 data lake, applying medallion architecture (Bronze → Silver → Gold), and serving a live business intelligence dashboard with **₹105B+ in revenue analytics**.

---

## 📌 Project Summary

This project simulates a **real-world enterprise data engineering scenario**: a parent FMCG company onboarding a child company (Sports Bar) onto their unified data platform. The pipeline handles data quality issues found in raw source files, standardises the child company's data model to match the parent, and merges everything into a shared Gold layer — enabling cross-company reporting in a single Databricks SQL Dashboard.

**What this demonstrates:**
- Production-style ETL with PySpark and Delta Lake
- Medallion architecture (Bronze / Silver / Gold) in Unity Catalog
- Incremental and full-load patterns with idempotent Delta merges
- Data quality engineering: typo correction, schema standardisation, date normalisation
- Star schema design and denormalised reporting views
- End-to-end: raw S3 files → cleansed tables → BI dashboard

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | AWS S3 |
| Compute | Databricks (Apache Spark) |
| Table Format | Delta Lake |
| Catalog | Unity Catalog (3-level namespace) |
| Language | PySpark, Spark SQL |
| Orchestration | Databricks Notebooks |
| BI / Reporting | Databricks SQL Dashboard |
| Data Model | Star Schema (Fact + 4 Dimensions) |

---

## 🏗️ Architecture

```
S3 Source Files
(customers / products / gross_price / orders)
        │
        ▼
┌─────────────────────────────────────────────┐
│              BRONZE LAYER                   │
│  Raw ingestion + metadata columns           │
│  (read_timestamp, file_name, file_size)     │
│  Delta with Change Data Feed enabled        │
└─────────────────────────────────────────────┘
        │
        ▼  (clean · validate · deduplicate · enrich)
┌─────────────────────────────────────────────┐
│              SILVER LAYER                   │
│  • Deduplication on business keys           │
│  • City typo correction (mapping dict)      │
│  • Date normalisation (4 format variants)   │
│  • Price validation + negative flipping     │
│  • Product division derivation              │
│  • Parent data model alignment              │
└─────────────────────────────────────────────┘
        │
        ▼  (select final columns → sb_ staging → Delta merge)
┌─────────────────────────────────────────────┐
│              GOLD LAYER (Child)             │
│  gold.sb_dim_customers                      │
│  gold.sb_dim_products                       │
│  gold.sb_dim_gross_price  (Window dedup)    │
│  gold.sb_fact_orders      (daily grain)     │
└──────────────────┬──────────────────────────┘
                   │  DeltaTable.merge() upsert
        ▼
┌─────────────────────────────────────────────┐
│              GOLD LAYER (Parent)            │
│  gold.dim_customers                         │
│  gold.dim_products                          │
│  gold.dim_gross_price                       │
│  gold.fact_orders   (monthly grain agg)     │
│  gold.dim_date      (generated date dim)    │
└──────────────────┬──────────────────────────┘
                   │
        ▼
┌─────────────────────────────────────────────┐
│            SERVING LAYER                    │
│  vw_fact_orders_enriched  (5-way JOIN)      │
│  → fmcg_dashboard (Databricks SQL)          │
└─────────────────────────────────────────────┘
```

---

## 📊 Dashboard Output

| KPI | Value |
|---|---|
| Total Revenue | ₹105.34 Billion |
| Total Quantity Sold | 34.13 Million units |
| Unique Customers | 54 |
| Average Selling Price | ₹4,043 |
| Top Revenue Channel | Retailer (78.49%) |

---

## 📁 Repository Structure

```
fmcg-databricks-pipeline/
│
├── README.md
│
├── setup/
│   ├── setup_catalog.ipynb          # Create fmcg catalog + bronze/silver/gold schemas
│   └── utilities.ipynb              # Shared schema name variables (%run in each notebook)
│
├── dimensions/
│   ├── 1_customers_data_processing.ipynb    # Bronze → Silver → Gold + merge to parent
│   ├── 2_products_data_processing.ipynb     # Bronze → Silver → Gold + merge to parent
│   ├── 3_pricing_data_processing.ipynb      # Bronze → Silver → Gold + merge to parent
│   └── dim_date_table_creation.ipynb        # Generate date dimension (2024–2025)
│
├── facts/
│   ├── 1_full_load_fact.ipynb              # Initial full load of fact_orders
│   └── 2_incremental_load_fact.ipynb       # Incremental load with monthly recalculation
│
├── sql/
│   ├── denormalise_table_query_fmcg.sql    # Creates vw_fact_orders_enriched (5-way JOIN view)
│   └── incremental_data_parent_company_query.sql  # COPY INTO for parent company incremental data
│
└── data/
    ├── dim_customers.csv
    ├── dim_products.csv
    ├── dim_gross_price.csv
    └── fact_orders.csv
```

---

## ⚙️ Pipeline Execution Order

Run notebooks in this sequence:

```
1. setup/setup_catalog.ipynb
2. setup/utilities.ipynb               ← (no direct run — %run'd by other notebooks)

3. dimensions/1_customers_data_processing.ipynb
4. dimensions/2_products_data_processing.ipynb
5. dimensions/3_pricing_data_processing.ipynb
6. dimensions/dim_date_table_creation.ipynb

7. facts/1_full_load_fact.ipynb        ← Run once for initial load
   OR
   facts/2_incremental_load_fact.ipynb ← Run for subsequent loads

8. sql/denormalise_table_query_fmcg.sql  ← Run in Databricks SQL editor
```

---

## 🔑 Key Engineering Decisions

### 1. Medallion Architecture (Bronze / Silver / Gold)
Each layer has a distinct purpose. Bronze preserves raw data with audit metadata. Silver is the cleaning layer. Gold is business-ready and optimised for reporting. This separation means failures can be re-run from any layer without re-ingesting from S3.

### 2. Change Data Feed (CDF) on All Delta Tables
All tables are created with `delta.enableChangeDataFeed = true`. This enables downstream CDC consumers and makes it possible to track exactly what changed between pipeline runs.

### 3. Idempotent Delta Merges
All writes to the Gold layer use `DeltaTable.merge()` rather than overwrite. The merge key is the natural business key (e.g. `customer_code`, `product_code`). Running the same notebook twice produces the same result — safe for retry on failure.

### 4. Daily → Monthly Grain Aggregation
The child company's fact data is at daily order grain. The parent `fact_orders` table is at monthly grain (date = first of month). Before merging, daily rows are aggregated using `F.trunc(date, "MM")` + `groupBy` + `SUM(sold_quantity)`. This is a critical design decision — without it, the merge key would never match the parent table.

### 5. Incremental Month Recalculation
For incremental loads, the pipeline identifies which months are touched by new data, pulls all existing daily rows for those months from `sb_fact_orders`, recalculates the full monthly total, and merges. This prevents partial-month inaccuracies when daily data arrives late or out of order.

### 6. Window Function for Latest Price Deduplication
Multiple price rows can exist per `product_code + year`. A Window function partitioned by `(product_code, year)` and ordered by `is_zero ASC, month DESC` selects the latest non-zero price. This ensures the reporting view uses accurate historical prices.

### 7. Landing Zone File Movement Pattern
Order CSV files land in `s3://sportsbar-final/orders/landing/`. After ingestion, each file is moved to `orders/processed/` using `dbutils.fs.mv()`. This prevents double-processing without needing a watermark table.

---

## 🧹 Data Quality Issues Handled

| Issue | Table | Fix Applied |
|---|---|---|
| City name typos (Bengaluruu, Hyderbad, NewDelhi…) | customers | `replace()` with mapping dict |
| Missing cities for 4 specific customers | customers | Lookup join with business-confirmed values |
| "Protien" misspelling in product name & category | products | `regexp_replace()` case-insensitive |
| Mixed date formats (4 variants) in pricing | gross_price | `F.coalesce(try_to_date(...))` |
| Negative & non-numeric gross prices | gross_price | Conditional cast + absolute value |
| Weekday prefix in order date ("Tuesday, July 01...") | orders | `regexp_replace()` strip prefix |
| Non-numeric customer IDs | orders | Regex validation, fallback to `999999` |
| Null order quantities | orders | Filter before write |
| product_id present but product_code needed | orders, gross_price | Inner join with `silver.products` |

---

## 📐 Data Model

```
                    ┌────────────┐
                    │  dim_date  │
                    │────────────│
                    │ month_start│
                    │ date_key   │
                    │ year       │
                    │ quarter    │
                    └─────┬──────┘
                          │
┌─────────────┐    ┌──────┴───────┐    ┌──────────────┐
│dim_customers│    │ fact_orders  │    │ dim_products │
│─────────────│    │──────────────│    │──────────────│
│customer_code├────┤ date (PK)    ├────┤ product_code │
│ customer    │    │ product_code │    │ division     │
│ market      │    │ customer_code│    │ category     │
│ platform    │    │ sold_quantity│    │ product      │
│ channel     │    └──────┬───────┘    │ variant      │
└─────────────┘           │            └──────────────┘
                          │
                   ┌──────┴────────┐
                   │dim_gross_price│
                   │───────────────│
                   │ product_code  │
                   │ price_inr     │
                   │ year          │
                   └───────────────┘
```

**Reporting view:** `vw_fact_orders_enriched` joins all 5 tables and computes `total_amount_inr = sold_quantity × price_inr` using year-matched pricing.

---

## 🚀 How to Run

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- AWS S3 bucket with source CSV files at `s3://sports-bar-2/`
- Databricks cluster with Delta Lake and PySpark

### Steps

```bash
# 1. Clone this repo
git clone https://github.com/YOUR_USERNAME/fmcg-databricks-pipeline.git

# 2. Upload notebooks to Databricks Workspace
# Databricks UI → Workspace → Import → select .ipynb files

# 3. Run setup first
# Open setup/setup_catalog.ipynb → Run All

# 4. Upload sample CSVs from data/ folder to your S3 bucket

# 5. Run dimension notebooks (1 → 2 → 3 → dim_date)

# 6. Run fact load notebook (full or incremental)

# 7. Run the SQL view creation in Databricks SQL Editor

# 8. Create your dashboard on top of vw_fact_orders_enriched
```

---

## 📂 Sample Data

Sample CSV files are included in the `data/` folder for local exploration. The full pipeline runs on files uploaded to S3.

| File | Rows (approx.) | Key Columns |
|---|---|---|
| `dim_customers.csv` | ~54 | customer_id, customer_name, city |
| `dim_products.csv` | ~100+ | product_id, product_code, category, variant |
| `dim_gross_price.csv` | ~200+ | product_id, month, gross_price |
| `fact_orders.csv` | ~34M qty | date, product_code, customer_code, sold_quantity |

---

## 🌱 What I Learned

- Designing **idempotent pipelines** that are safe to re-run without side effects
- Handling **real-world data quality** — messy dates, typos, nulls, type mismatches
- The importance of **grain alignment** between child and parent fact tables
- Using **Delta Lake merge patterns** for SCD Type 1 dimension updates
- Building a **star schema** optimised for analytical queries
- Writing **incremental load logic** that handles late-arriving and out-of-order data

---

## 👤 Author

**[Omprakash Choudhary]**
Aspiring Data Engineer | PySpark · Databricks · Delta Lake · SQL

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/omprakash-choudhary-a95361155/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/omprakashchoudhry)

---

*Built as a hands-on data engineering project to demonstrate end-to-end pipeline development on the Databricks Lakehouse Platform.*

