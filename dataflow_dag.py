from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

PROJECT_ID = "learn-gcloud-462613"
BUCKET = "us-central1-dev-77c518d1-bucket"
REGION = "us-central1"

default_args = {
    "start_date": days_ago(1),
    "retries": 0,
}

with models.DAG(
    dag_id="dataflow_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id="check_file_exists",
        bucket=BUCKET,
        object="data/users.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=60,
        poke_interval=15,
    )

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_dataflow",
        runner="DataflowRunner",
        py_file=f"gs://{BUCKET}/dags/dataflow_gcs_to_bq.py",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": f"gs://{BUCKET}/temp",
            "staging_location": f"gs://{BUCKET}/staging",
            "input": f"gs://{BUCKET}/data/users.csv",
            "dataset": "dataflowtables"
        },
        py_interpreter='python3'
    )

    check_file_exists >> run_dataflow
