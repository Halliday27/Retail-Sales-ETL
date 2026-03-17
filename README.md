# Retail Sales ETL on AWS

A beginner-friendly, end-to-end ETL project using **Amazon S3** for raw files and **Amazon RDS PostgreSQL** as the analytics database.

## Course learning outcomes

By completing this project you will be able to:
- Explain source → raw → transform → target → analytics flow.
- Build a simple AWS data stack with S3 + RDS PostgreSQL.
- Implement a dimensional model (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`).
- Run ETL with reproducible, idempotent loading.
- Write SQL queries for business-ready reporting.

## Architecture

```text
CSV files
   ↓
Amazon S3 (raw/) or local data/
   ↓
Python ETL job (extract → transform → load)
   ↓
Amazon RDS PostgreSQL (dim/fact tables)
   ↓
SQL analytics queries
```

## Project structure

```text
Retail-Sales-ETL/
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── etl/
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
├── sql/
│   ├── create_tables.sql
│   └── analytics_queries.sql
├── .env.example
├── requirements.txt
└── README.md
```

## Dataset

Sample files are included in `data/`:
- `customers.csv`
- `products.csv`
- `orders.csv`

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy environment variables:

```bash
cp .env.example .env
```

4. Update `DATABASE_URL` in `.env` to your RDS PostgreSQL connection string.

## Run ETL

### Local source files (default)

```bash
export $(cat .env | xargs)
python -m etl.main
```

### S3 source files

Set:
- `SOURCE_MODE=s3`
- `S3_BUCKET=your-bucket`
- `S3_PREFIX=raw`

Expected S3 object keys:
- `raw/customers/customers.csv`
- `raw/products/products.csv`
- `raw/orders/orders.csv`

Then run:

```bash
python -m etl.main
```

## ETL behavior

### Extract
- Reads CSVs from local disk or S3.

### Transform
- Removes bad/duplicate keys.
- Enforces `quantity > 0`.
- Converts `order_date` to date.
- Creates date dimension (`date_key = YYYYMMDD`).
- Calculates `total_amount = quantity * unit_price`.

### Load (idempotent)
- Creates target tables if missing.
- Upserts dimensions by business keys.
- Upserts fact rows by `order_id` to support safe reruns.

## SQL model

- `dim_customer(customer_key, customer_id, customer_name, city, country)`
- `dim_product(product_key, product_id, product_name, category, unit_price)`
- `dim_date(date_key, full_date, year, month, day)`
- `fact_sales(sales_key, order_id, date_key, customer_key, product_key, quantity, unit_price, total_amount)`

Create DDL: `sql/create_tables.sql`

## Analytics checks

Run queries in `sql/analytics_queries.sql`:
1. Total sales by day
2. Top-selling products
3. Sales by customer city

## Suggested learning path

1. **Week 1**: define source, rules, target, outputs.
2. **Week 2**: create S3 + RDS.
3. **Week 3**: create schema.
4. **Week 4**: implement extract/transform.
5. **Week 5**: implement load + SQL validation.
6. **Week 6**: add scheduling, logging, and data quality checks.

## Real-world habits used in this repo

- Keep raw data immutable.
- Separate raw and curated logic.
- Load dimensions before facts.
- Make reruns safe with upserts.
- Log each major ETL stage.
