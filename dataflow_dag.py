from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from google.cloud import bigquery


PROJECT_ID = "learn-gcloud-462613"
STAGE_DATASET = "stage"
FINAL_DATASET = "final"
BUCKET = "us-central1-dev-548b1475-bucket"
REGION = "us-central1"


default_args = {
    "start_date": days_ago(0),
    "retries": 0,
}


def run_transformation():
    client = bigquery.Client(project=PROJECT_ID)

    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age_group", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("birth_year", "INTEGER", mode="NULLABLE"),
    ]

    table_id = f"{PROJECT_ID}.{FINAL_DATASET}.transformed_table"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

    transformed_table_id = f"{FINAL_DATASET}.transformed_table"
    stage_table_id = f"{STAGE_DATASET}.main_table"

    copy_table_query = f"""
        INSERT INTO `{transformed_table_id}` (name, age, email)
        SELECT name, age, email
        FROM `{stage_table_id}`;
    """

    age_group_query = f"""
        UPDATE `{transformed_table_id}`
        SET age_group =
            CASE
                WHEN age > 50 THEN "Senior Citizen"
                WHEN age < 20 THEN "Teenager"
                ELSE "Adult"
            END
        WHERE age IS NOT NULL;
    """

    birth_year_query = f"""
        UPDATE `{transformed_table_id}`
        SET birth_year = 2025 - age
        WHERE age IS NOT NULL;
    """
    
    client.query(copy_table_query).result()
    client.query(age_group_query).result()
    client.query(birth_year_query).result()


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
            "dataset": STAGE_DATASET
        },
        py_interpreter='python3'
    )


    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_transformation
    )


    check_file_exists >> run_dataflow >> transform_task
