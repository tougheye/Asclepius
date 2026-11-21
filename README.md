# Asclepius — Provider Master Data Harmonization

This Master Data Management (MDM) project takes in fragmented, inconsistent physician provider records from multiple legacy source systems (Acquired Clinic Alpha, Legacy HR DB, Old Billing System) and harmonizes them into a single, trusted **Golden Record** for each unique provider. Built entirely on Databricks, the project demonstrates a complete multi-layer MDM pipeline — from synthetic data generation through standardization, data-quality scoring, fuzzy-matching, survivorship, and golden record creation.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Data Layers](#data-layers)
4. [Key Business Decisions](#key-business-decisions)
5. [Notebooks & Components](#notebooks--components)
6. [Job & Pipeline Orchestration](#job--pipeline-orchestration)
7. [End Product — Golden Records](#end-product--golden-records)
8. [Unity Catalog Tables](#unity-catalog-tables)
9. [How to Run](#how-to-run)

---

## Architecture Overview

The pipeline follows a classic **Bronze → Silver → Gold** medallion architecture:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        NPPES API (real provider data)                             │
│                                      │                                            │
│                         ┌────────────▼─────────────┐                              │
│                         │    Data_generator.ipynb   │  ← JOB: cms_mdm_bronze_      │
│                         │  Fetch + corrupt + ingest  │     with_specialty           │
│                         └────────────┬─────────────┘                              │
│                                      │                                            │
│                           ┌──────────▼──────────┐                                 │
│                           │   BRONZE LAYER       │                                 │
│                           │  cms_mdm_bronze.     │                                 │
│                           │  cms_mdm_ingest.     │                                 │
│                           │  ca_doctor_list      │                                 │
│                           └──────────┬──────────┘                                 │
│                                      │                                            │
│                    ┌─────────────────▼──────────────────┐                         │
│                    │  SILVER LAYER                       │                         │
│                    │  silver_transform.py (SDP)          │  ← Spark Declarative     │
│                    │  Asclepius_silver_data_cleanup      │     Pipeline             │
│                    │  Address + phone standardization    │                         │
│                    │  → cms_mdm_silver.                  │                         │
│                    │    doctor_list_silver               │                         │
│                    └─────────────────┬──────────────────┘                         │
│                                      │                                            │
│                    ┌─────────────────▼──────────────────┐                         │
│                    │  GOLD LAYER                         │                         │
│                    │  Asclepius_gold_data_quality        │                         │
│                    │  DQ scoring, fuzzy matching,        │                         │
│                    │  survivorship, golden record build  │                         │
│                    │  → cms_mdm_gold tables              │                         │
│                    └──────────────────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
Asclepius – Provider Master Data Harmonization/
└── Asclepius/
    ├── README.md                          ← This file
    ├── Data_generator.ipynb               ← Bronze ingestion (NPPES API + synthetic corruption)
    ├── Asclepius_silver_data_cleanup.ipynb ← Silver standardization (interactive notebook)
    ├── Asclepius_gold_data_quality.ipynb   ← Gold layer: DQ scoring, matching, golden records
    └── transformations/
        ├── Data_ingestion_job.py           ← Job creation/management script (Databricks SDK)
        └── silver_transform.py.ipynb       ← Silver standardization (Spark Declarative Pipeline)
```

---

## Data Layers

### Bronze Layer — Raw Ingestion

**Purpose:** Ingest real provider data from the **NPPES NPI Registry API** and deliberately introduce data quality issues to simulate legacy system fragmentation.

**Process:**

* Fetches real physician records from the NPPES API (`https://npiregistry.cms.hhs.gov/api/`) filtered by state (default: CA) and taxonomy/specialty.
* Generates 1–3 synthetic "legacy" records per real provider to simulate fragmentation across source systems.
* Introduces intentional data corruptions:
  * **Missing NPIs** — 20% of records have NPI set to null
  * **Name typos** — 30% chance of character drop or swap in first and last names
  * **Inconsistent phone formatting** — randomly applies dashes, parentheses, dots, country codes, or no formatting
  * **Missing phone/address** — 15% chance each of being nulled out
  * **Random last-updated dates** — within the last 5 years (for survivorship testing)
  * **Random source system assignment** — `Acquired_Clinic_Alpha`, `Legacy_HR_DB`, or `Old_Billing_System`
* Writes to Unity Catalog table `cms_mdm_bronze.cms_mdm_ingest.ca_doctor_list` (append mode, with specialty deduplication check).

### Silver Layer — Standardization

**Purpose:** Standardize raw Bronze data into a clean, consistent format suitable for matching and analytics.

Two implementations exist:

1. **Asclepius_silver_data_cleanup.ipynb** — Interactive pandas-based notebook
2. **silver_transform.py.ipynb** — Spark Declarative Pipeline (SDP) version for streaming/automated execution

**Standardization steps:**

* **Address splitting** — Separates `Practice_Address` into `Practice_Address_line_1` (street address) and `Practice_Address_line_2` (suite/apt/floor details) based on keyword detection (`SUITE`, `STE`, `APT`, `UNIT`, `BLDG`, `FLOOR`, `FL`, `#`).
* **Street type normalization** — Maps abbreviations to full forms (`ST` → `Street`, `AVE` → `Avenue`, `BLVD` → `Boulevard`, `RD` → `Road`, `DR` → `Drive`, `LN` → `Lane`, `CT` → `Court`, `PL` → `Place`, `PKWY` → `Parkway`, `WAY` → `Way`).
* **Directional standardization** (SDP version) — Expands directional indicators (`N` → `NORTH`, `NE` → `NORTHEAST`, etc.).
* **Phone number cleaning** — Extracts extensions, strips non-digit characters, normalizes US 11-digit numbers to 10-digit format, validates length.
* **Output** — Standardized columns: `Legacy_System_ID`, `NPI`, `First_Name`, `Last_Name`, `Practice_Address`, `Practice_Address_line_1`, `Practice_Address_line_2`, `City`, `State`, `Zip`, `Phone`, `Phone_std`, `Phone_Ext`, `Last_Updated_Date`, `Source_System`.
* **Target table:** `cms_mdm_silver.cms_mdm_silver.doctor_list_silver` (overwrite mode).

### Gold Layer — Data Quality, Matching & Golden Records

**Purpose:** Score data quality, fuzzy-match duplicate providers, apply survivorship rules, and produce authoritative Golden Records.

This is the most complex layer, performed in `Asclepius_gold_data_quality.ipynb`:

#### Step 1: Data Quality Scoring

The silver dataset is scored on two MDM data quality dimensions:

* **Completeness Score** — Each record is scored on how many of 8 critical MDM fields are populated: `NPI`, `First_Name`, `Last_Name`, `Practice_Address_line_1`, `City`, `State`, `Zip`, `Phone_std`. Starts at 100; loses points for each missing field.
* **Uniqueness Score** — Starts at 100; deducts 10 points each for duplicate NPI, duplicate practice address (per name + address + zip), and duplicate phone number (per name + phone).
* **Composite Data Quality Score (DQS)** — `DQS = (Completeness + Uniqueness) × 0.50`, equally weighting both dimensions.

#### Step 2: Fuzzy Name Matching

* Uses **RapidFuzz** (`token_sort_ratio` scorer) to compare all unique provider full names.
* Pairs with similarity score > 80 are flagged as potential duplicates.
* Match scores are categorized into confidence levels:

  | Confidence Level | Score Threshold |
  | --- | --- |
  | High | ≥ 93 |
  | Medium | ≥ 88 |
  | Low | ≥ 70 |

#### Step 3: Survivorship Rules

Survivorship rules determine which value "wins" when multiple records refer to the same provider:

* **Name survivorship:**
  * Rule 1 — The most frequently occurring name variant wins.
  * Rule 2 — If counts are tied, the longer name wins.
  * If both count and length are tied → flagged as `Need Manual Review`.
  * Only `High` confidence matches are auto-resolved; `Medium` and `Low` go to manual review.

* **NPI survivorship:**
  * For confirmed golden names with missing NPIs, the NPI is backfilled from matched records sharing the same golden name.
  * Remaining unmatched NPIs are flagged as `Need Manual Review`.

* **Address survivorship:**
  * Rule 1 — If address is null, the reported address from the same golden NPI survives.
  * Rule 2 — If multiple addresses exist for the same golden NPI, the one with the most recent `Last_Updated_Date` wins.

* **Phone number survivorship:**
  * Rule 1 — If a golden NPI has multiple phone numbers (including null), the most recently used phone number survives.
  * Rule 2 — Remaining null phone numbers are filled from high-confidence golden name matches.
  * Rule 3 — If no match exists, flagged for manual review.

#### Step 4: Golden Record Output

* Records with zero manual review flags are deduplicated by `golden_NPI` and written as the final **Golden Record table**.
* Records requiring manual review are written to a separate **Review table**.
* A **Conversion table** maps `Legacy_System_ID` → `golden_NPI` for cross-referencing legacy systems with the harmonized records.

---

## Key Business Decisions


Standardization rules were applied **early in the Silver layer**, as soon as raw Bronze data landed rather than deferring cleanup to the Gold layer. Because every downstream consumer (data quality scoring, fuzzy matching, survivorship rules, golden record creation) operates on already-standardized data, each record is cleaned **once** instead of being re-cleaned at every downstream step. This early-cleanup strategy saves significant further processing down the road: matching logic compares normalized addresses and phones directly, without each stage having to handle formatting variations on its own.


The Silver cleanup was deliberately **synchronized**: standardization functions were designed so that different column variables (addresses, street types, directionals, phone formats) are aligned through a single coordinated set of rules. Instead of ad-hoc, per-column handling, all columns flow through the same aligned standardization logic and produce a consistent, uniform cleanup output. This synchronized design avoids redundant passes over the data; saving computation costs across the pipeline.

---

## Notebooks & Components

| Asset | Type | Location | Purpose |
| --- | --- | --- | --- |
| Data_generator | Notebook | `/Asclepius/Data_generator.ipynb` | Fetches real NPPES API data, corrupts it, and writes to Bronze |
| Asclepius_silver_data_cleanup | Notebook | `/Asclepius/Asclepius_silver_data_cleanup.ipynb` | Interactive Silver standardization (pandas-based) |
| silver_transform.py | Notebook (SDP) | `/Asclepius/transformations/silver_transform.py.ipynb` | Spark Declarative Pipeline version of Silver standardization |
| Asclepius_gold_data_quality | Notebook | `/Asclepius/Asclepius_gold_data_quality.ipynb` | Gold layer: DQ scoring, fuzzy matching, survivorship, golden records |
| Data_ingestion_job.py | Script (SDK) | `/Asclepius/transformations/Data_ingestion_job.py` | Databricks SDK script to create/manage the ingestion job |

---

## Job & Pipeline Orchestration

### Job: `cms_mdm_bronze_with_specialty`

A Databricks **Lakeflow Job** (Job ID: `1072049877910116`) orchestrates the Bronze layer ingestion:

| Property | Value |
| --- | --- |
| **Job Name** | `cms_mdm_bronze_with_specialty` |
| **Task Key** | `cms_mdm_data_ingestion` |
| **Notebook** | `Data_generator.ipynb` |
| **Base Parameters** | `specialty=Neurology`, `catalog=cms_mdm_bronze`, `schema=cms_mdm_ingest`, `table_name=ca_doctor_list` |
| **Performance Target** | `PERFORMANCE_OPTIMIZED` |
| **Queue** | Enabled |
| **Email Notifications** | On start, success, and failure |
| **Trigger** | One-time (manual or API-triggered) |

The job is configured and managed via the **Databricks Python SDK** in `Data_ingestion_job.py`, which uses `WorkspaceClient().jobs.reset()` to update the job settings. The `specialty` parameter can be changed to ingest providers from different medical specialties (e.g., Pediatrics, Cardiology, Neurology) in separate runs.

### Spark Declarative Pipeline (SDP)

The `silver_transform.py.ipynb` notebook contains a **Spark Declarative Pipeline** definition that creates a streaming Silver table (`cms_mdm_silver.cms_mdm_silver.silver_streaming_table`) from the Bronze source. It uses:

* `@dp.table` decorator to define the streaming table
* `spark.readStream.table()` for continuous Bronze ingestion
* PySpark UDFs for address standardization and phone cleaning
* Automatic schema evolution and incremental processing

### Execution Flow

1. **Run the job** `cms_mdm_bronze_with_specialty` (or trigger `Data_generator.ipynb` directly) to populate Bronze.
2. **Run the Silver pipeline** — either `Asclepius_silver_data_cleanup.ipynb` (batch) or `silver_transform.py.ipynb` (SDP streaming) to standardize Bronze → Silver.
3. **Run `Asclepius_gold_data_quality.ipynb`** to score data quality, match duplicates, apply survivorship rules, and produce Golden Records.

---

## End Product — Golden Records

The final output of the Asclepius pipeline is a set of **Golden Records** — one authoritative, de-duplicated, and harmonized record per unique healthcare provider. Each Golden Record contains:

| Golden Field | Description |
| --- | --- |
| `golden_NPI` | The surviving National Provider Identifier |
| `golden_name` | The surviving provider full name |
| `Golden_Practice_Address_line_1` | Standardized primary practice address |
| `Golden_Practice_Address_line_2` | Suite/floor/unit details |
| `Golden_City` | Practice city |
| `Golden_State` | Practice state |
| `Golden_Zip` | Practice ZIP code |
| `Golden_Phone_std` | Standardized 10-digit phone number |
| `Golden_Phone_Ext` | Phone extension (if available) |
| `Legacy_System_ID` | Original legacy system identifier (for cross-referencing) |

Alongside the Golden Records, two supporting tables are produced:

* **Conversion Table** — Maps each `Legacy_System_ID` to its `golden_NPI`, enabling downstream systems to link legacy records to the harmonized golden provider.
* **Review Table** — Contains records that could not be auto-resolved by the survivorship rules and require manual data stewardship review.

---

## Unity Catalog Tables

| Layer | Catalog | Schema | Table | Description |
| --- | --- | --- | --- | --- |
| Bronze | `cms_mdm_bronze` | `cms_mdm_ingest` | `ca_doctor_list` | Raw, intentionally corrupted provider records |
| Silver | `cms_mdm_silver` | `cms_mdm_silver` | `doctor_list_silver` | Standardized addresses, phones, and columns |
| Silver (SDP) | `cms_mdm_silver` | `cms_mdm_silver` | `silver_streaming_table` | Streaming version of the Silver table |
| Gold | `cms_mdm_gold` | `golden_record` | `golden_table` | Final de-duplicated Golden Records |
| Gold | `cms_mdm_gold` | `conversion_table` | `conversion_table` | Legacy System ID → Golden NPI mapping |
| Gold | `cms_mdm_gold` | `review_tables` | `review_table` | Records requiring manual review |

---

## How to Run

### Prerequisites

* Databricks workspace with Unity Catalog enabled
* Serverless or all-purpose compute (Python 3.12+)
* Unity Catalog schemas: `cms_mdm_bronze.cms_mdm_ingest`, `cms_mdm_silver.cms_mdm_silver`, `cms_mdm_gold` (with `golden_record`, `conversion_table`, `review_tables` schemas)
* The `rapidfuzz` library (installed via `%pip install rapidfuzz` in the Gold notebook)

### Steps

1. **Ingest Bronze data:**
   * Run the job `cms_mdm_bronze_with_specialty` from the Jobs UI, or
   * Open `Data_generator.ipynb`, set widget parameters (catalog, schema, table_name, state, specialty), and run all cells.

2. **Standardize to Silver:**
   * Run `Asclepius_silver_data_cleanup.ipynb` for batch processing, or
   * Configure and run the Spark Declarative Pipeline from `silver_transform.py.ipynb`.

3. **Generate Golden Records:**
   * Open `Asclepius_gold_data_quality.ipynb` and run all cells sequentially.
   * The notebook reads from Silver, scores data quality, performs fuzzy matching, applies survivorship rules, and writes three Gold tables.

4. **Review:**
   * Query `cms_mdm_gold.golden_record.golden_table` for the harmonized provider records.
   * Query `cms_mdm_gold.review_tables.review_table` for records needing manual review.
   * Query `cms_mdm_gold.conversion_table.conversion_table` for the legacy-to-golden crosswalk.

---

## Key Technologies

* **Databricks** — Compute, notebooks, Unity Catalog, and job orchestration
* **Apache Spark / PySpark** — Distributed data processing and streaming
* **Spark Declarative Pipelines (SDP)** — Declarative streaming transformations for the Silver layer
* **RapidFuzz** — Fuzzy string matching for name deduplication
* **NPPES NPI Registry API** — Real provider data source
* **Unity Catalog** — Three-catalog governance structure (Bronze, Silver, Gold)
* **Databricks SDK for Python** — Job configuration and management
* **Pandas** — Interactive data manipulation in the Gold layer 