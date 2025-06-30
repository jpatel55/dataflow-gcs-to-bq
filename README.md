# GCP Data Ingestion & Transformation Pipeline (Beam + Dataflow + Airflow)

This project implements a **serverless, production-grade data pipeline** using:

- **Apache Beam** (Python SDK)
- **Google Cloud Dataflow** (runner)
- **Google Cloud Storage (GCS)** – input CSV
- **Google BigQuery** – validated & transformed output
- **Apache Airflow / Cloud Composer** – orchestration

---

## ✅ What It Does

1. **Reads** a CSV from GCS
2. **Validates** each record:
   - `name` is not empty
   - `email` contains `@`
   - `age` is a valid integer
3. **Tags records** as:
   - ✅ Valid → goes to `main_table`
   - ❌ Invalid → goes to `error_table`
4. **Generates** an audit log with:
   - File name
   - Timestamp
   - Valid record count
5. **Transforms** validated data into a final BigQuery table with:
   - `age_group`: based on age
   - `birth_year`: computed from age

---

## 🗃️ BigQuery Tables

| Table Name                          | Description                          |
|-------------------------------------|--------------------------------------|
| `stage.main_table`                  | All validated records                |
| `stage.error_table`                 | Invalid rows (missing/wrong data)    |
| `stage.audit_log_table`            | Metadata about ingestion             |
| `final.transformed_table`          | Output with enriched fields          |

> All tables are created automatically if they don’t exist.

---

## 📁 Files in This Repo

| File | Description |
|------|-------------|
| `dataflow_dag.py` | Airflow DAG that triggers the Beam pipeline and performs the BigQuery transformation |
| `dataflow_gcs_to_bq.py` | Apache Beam pipeline: reads, validates, writes to BQ |

---

## ⚙️ How It Works

### 🐍 Beam Pipeline (`dataflow_gcs_to_bq.py`)

- Parses each CSV row
- Validates fields
- Uses Beam's `TaggedOutput` to separate valid/invalid
- Writes to BigQuery via `WriteToBigQuery`
- Counts valid rows and logs audit info

### ☁️ Airflow DAG (`dataflow_dag.py`)

- Waits for `users.csv` to appear in GCS
- Runs the Beam pipeline using `BeamRunPythonPipelineOperator`
- Uses BigQuery Python client
- Computes `age_group` and `birth_year`
- Inserts enriched rows into `final.transformed_table`

---

## 📥 Input Format

CSV should look like:

```csv
name,age,email
Alice,25,alice@example.com
Bob,17,bob[at]example.com
Carol,65,carol@example.com
