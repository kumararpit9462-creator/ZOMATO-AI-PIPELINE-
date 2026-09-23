# ZOMATO-AI-PIPELINE-
# Zomato Data Platform

A batch data platform that takes food-delivery data from raw CSVs to AI-powered analytics, organized by pipeline lane: **infra → ingest → transform → orchestrate → intelligence**.

```
CSVs → Amazon S3 → Snowflake (Bronze) → dbt (Silver → Gold) → Airflow → OpenAI (enrichment · RAG · text-to-SQL)
```

Raw data lands in an S3 data lake and is loaded into Snowflake through a keyless storage integration. dbt models it through medallion layers: Bronze tables loaded with `COPY INTO`, cleaned Silver views, and business-ready Gold tables (dimensions, incremental facts, aggregate marts). Airflow runs everything as one daily DAG. An intelligence layer on top uses OpenAI to turn free-text reviews into structured columns, answer questions grounded in real reviews (RAG), and translate plain English into SQL. Streamlit serves the apps.

<img width="1797" height="875" alt="ChatGPT Image Sep 24, 2026, 01_13_27 AM" src="https://github.com/user-attachments/assets/4483a3f7-8fc7-42ea-9798-14259e7caad3" />


---

## Quickstart

```bash
# 1. Infra: run infra/snowflake/01 → 05 in Snowsight (AWS side: see docs/aws-snowflake-handshake.md)

# 2. Transform
make dbt            # dbt debug + dbt build --exclude tag:ai

# 3. Orchestrate
make up             # Airflow on Docker → http://localhost:8080 → un-pause the DAG → Trigger

# 4. Intelligence
make enrich         # LLM enrichment of reviews
make apps           # launch the Streamlit apps
```

Copy the `.env.example` files, fill in your credentials, and never commit them.

---

## The lanes at a glance

| Lane | Where | What |
|---|---|---|
| **Source** | `data/` (local, not committed) | 4 dimension CSVs (restaurants, users, food, menu) + 3 fact files: **10M orders**, **~23M order items**, **300K free-text reviews** |
| **Lake** | Amazon S3 | One bucket, one `raw/<table>/` folder per CSV |
| **Bronze** | Snowflake `ZOMATO.RAW` | `COPY INTO` from S3 via a storage integration |
| **Silver** | Snowflake `ZOMATO.STAGING` | dbt views that clean, type, and rename every source |
| **Gold** | Snowflake `ZOMATO.MARTS` | Dimensions, incremental facts (MERGE), business marts, one SCD2 snapshot |
| **AI** | Snowflake `ZOMATO.AI` | LLM-enriched reviews, RAG chat, text-to-SQL |
| **Orchestration** | Airflow (Docker) | One daily DAG: load → transform → enrich → AI mart |

**Tech stack:** Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (`gpt-4o-mini`, `text-embedding-3-small`) · Streamlit

---

## Repository structure

```
zomato-data-platform/
├── infra/
│   ├── aws/                              # IAM policy + role trust policies (S3 ↔ Snowflake)
│   │   ├── policies/s3-read-only.json
│   │   └── trust/{bootstrap,final}.json
│   └── snowflake/                        # Setup SQL, run in Snowsight in order
│       ├── 01_bootstrap_wh_db_roles.sql
│       ├── 02_s3_storage_integration.sql
│       ├── 03_external_stage_file_format.sql
│       ├── 04_bronze_tables.sql
│       └── 05_bronze_load.sql
├── transform/                            # dbt project
│   ├── models/
│   │   ├── bronze_sources/               # source definitions + tests
│   │   ├── silver/                       # 7 cleaned views
│   │   └── gold/                         # dims, incremental facts, marts
│   └── macros/                           # custom schema-name macro
├── orchestration/                        # Airflow 3 on Docker
│   ├── dags/daily_zomato_pipeline.py     # the pipeline DAG (4 tasks)
│   ├── docker/{Dockerfile, docker-compose.yaml}
│   └── .env.example
├── intelligence/                         # AI layer
│   ├── enrichment/review_enrichment.py   # LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   └── apps/
│       ├── review_rag_app.py             # chat with your reviews
│       └── nl_to_sql_app.py              # chat with your warehouse
├── docs/
│   ├── architecture.png
│   ├── aws-snowflake-handshake.md
│   └── medallion-design.md
├── Makefile
└── README.md
```

> `data/` (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are intentionally not committed. See [Dataset](#dataset--credits).

---

## How it works

### 1 · Lake: data lands in S3

The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/`, one folder per table: `restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`.

### 2 · Ingest: S3 → Snowflake with no stored keys

Snowflake reads the bucket through a **storage integration** and an IAM role, so no AWS keys are stored anywhere. The Snowflake side is [`infra/snowflake/02_s3_storage_integration.sql`](infra/snowflake/02_s3_storage_integration.sql); the AWS JSON lives in [`infra/aws/`](infra/aws/).

| File | Purpose |
|---|---|
| `policies/s3-read-only.json` | IAM policy `zomato-s3-read`, read-only access to the bucket |
| `trust/bootstrap.json` | Placeholder trust policy used when the role is first created |
| `trust/final.json` | Final trust policy with Snowflake's IAM user ARN and external ID |

Order of operations: create the AWS policy and role → create the Snowflake `STORAGE INTEGRATION` pointing at the role ARN → run `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` → paste both into the role's trust policy.

Two gotchas: the trust `Principal` must be Snowflake's IAM user ARN, not `:root`, and never re-run `CREATE OR REPLACE` on the integration afterward, because it regenerates the external ID and breaks the trust. Full walkthrough in [`docs/aws-snowflake-handshake.md`](docs/aws-snowflake-handshake.md).

### 3 · Bronze: `COPY INTO`

[`04_bronze_tables.sql`](infra/snowflake/04_bronze_tables.sql) defines tables whose column order matches each CSV, and [`05_bronze_load.sql`](infra/snowflake/05_bronze_load.sql) pulls each file from the stage into `ZOMATO.RAW`: 10M orders, ~23M order items, 300K reviews.

### 4 · Silver and Gold: dbt

- **Silver:** one view per source. It parses the messy restaurant dimension (`--` → null, `₹ 200` → 200), lowercases emails, derives `is_delivered`, and so on.
- **Gold dimensions:** `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, and a generated `dim_date` calendar.
- **Gold facts (incremental):** `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so a re-run processes only new rows instead of rebuilding 10M+.
- **Gold marts:** one table per business question: daily city revenue (GMV / AOV / cancel rate), restaurant performance, delivery SLA (p50/p90 by city and hour), review insights.
- **Tests:** `unique`, `not_null`, `relationships`, `accepted_values`, plus a singular reconciliation test. `dbt build` runs models and tests in dependency order.

More in [`docs/medallion-design.md`](docs/medallion-design.md).

### 5 · Orchestration: Airflow

One daily DAG, [`daily_zomato_pipeline`](orchestration/dags/daily_zomato_pipeline.py), runs the whole thing as a single graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (OpenAI enrichment)   (AI mart)
```

Credentials never touch the code. `docker-compose` injects `SNOWFLAKE_*` env vars (read by dbt's `profiles.yml` via `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task.

### 6 · Intelligence: three capabilities

1. **LLM enrichment** ([`review_enrichment.py`](intelligence/enrichment/review_enrichment.py)): LLM as a transformation step. It reads review text, asks `gpt-4o-mini` for structured JSON (sentiment + topic), and writes it to `ZOMATO.AI.REVIEW_ENRICHED`, which dbt then models into `mart_review_insights` like any other table. It is idempotent and sample-capped (`SAMPLE_N`), so you never pay twice for the same review.
2. **RAG** ([`review_rag_app.py`](intelligence/apps/review_rag_app.py)): chat with your reviews. It embeds reviews, retrieves the most similar ones for a question, and answers from them with sources.
3. **Text-to-SQL** ([`nl_to_sql_app.py`](intelligence/apps/nl_to_sql_app.py)): chat with your warehouse. The LLM gets the marts' schema and writes Snowflake SQL for an English question. A SELECT-only guard validates it before it runs as `DBT_ROLE`.

---

## Design decisions

- **Keyless S3 access:** a storage integration plus an IAM trust policy means no long-lived AWS credentials anywhere.
- **Incremental MERGE facts:** re-runs touch only new rows, which keeps a 10M-order table cheap to maintain.
- **Idempotent enrichment:** reviews already enriched are skipped, which keeps LLM cost predictable.
- **SELECT-only guard on text-to-SQL:** generated SQL is validated before execution and runs under a restricted role.
- **Secrets via environment:** nothing sensitive is committed. Everything comes from `.env` files or env vars.

---

## Running each piece manually

```bash
# dbt
cd transform
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd orchestration
cp .env.example .env         # fill SNOWFLAKE_*, OPENAI_API_KEY, SAMPLE_N
docker compose -f docker/docker-compose.yaml build
docker compose -f docker/docker-compose.yaml up -d
# http://localhost:8080 → un-pause the DAG → Trigger

# Intelligence
export OPENAI_API_KEY=sk-...
python intelligence/enrichment/review_enrichment.py
streamlit run intelligence/apps/review_rag_app.py
streamlit run intelligence/apps/nl_to_sql_app.py
```

---

## Dataset & credits

- **Dataset and project slides:** [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing). Download the CSVs and place them under `data/` (too large to commit).
- **Original tutorial:** this project is based on [Darshil Parmar's video walkthrough](https://youtu.be/kYwaNMQ3XT8?si=Ge8ilVxkmGQS6iIg) and his original repository. I re-organized it by pipeline lane and extended it (see the Makefile, docs, and design notes).
