# GCP Data Ingestion Pipeline (Beam + Dataflow + Airflow)

This project is a serverless data ingestion pipeline built using:

- **Apache Beam** (Python)
- **Google Cloud Dataflow** (runner)
- **Cloud Storage** (input CSV)
- **BigQuery** (output)
- **Apache Airflow / Cloud Composer** (orchestration)

---

## ✅ What It Does

- Reads a CSV from GCS
- Validates records:
  - `name` is not empty
  - `email` contains `@`
  - `age` is an integer
- Writes:
  - ✅ Valid → `main_table`
  - ❌ Invalid → `error_table`
  - 📊 Audit log → `audit_log_table`

All tables are created automatically in BigQuery.

---

## 🗂️ Files

- `dataflow_dag.py`: Airflow DAG
- `dataflow_gcs_to_bq.py`: Beam pipeline script
