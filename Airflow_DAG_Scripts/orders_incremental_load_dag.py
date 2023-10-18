from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
import pendulum
from datetime import datetime


default_args = {
    'owner': 'Airflow',
    "start_date": pendulum.datetime(2023, 10, 12, tz="Asia/Kolkata"),
}

dag = DAG(
    dag_id='orders_incremental_load_to_deltatable',
    default_args=default_args,
    schedule_interval='50 12 * * *',
    catchup=False
)

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
batch_name = f'orders-incremental-load-{timestamp}'
pyspark_script = "gs://ecommerce_de_project_97/Spark_Folder/scripts/orders_tables_incremental_load_pyspark_script.py"

jar_file_uri = "gs://ecommerce_de_project_97/Spark_Folder/dependencies/mysql-connector-j-8.1.0.jar"



submit_batch_task = DataprocCreateBatchOperator(
    task_id='orders_incremental_load',
    region='us-central1',
    project_id='my-ecommerce-de-project-97',
    batch={
        "pyspark_batch": {
            "main_python_file_uri": pyspark_script,
            "jar_file_uris": [jar_file_uri],
        },
        "environment_config": {
            "execution_config": {
                "service_account": "dataproc-sa-deproject@my-ecommerce-de-project-97.iam.gserviceaccount.com",
                "subnetwork_uri": "projects/my-ecommerce-de-project-97/regions/us-central1/subnetworks/vpc-test",
            },
        },
        "runtime_config": {
            "version": "2.1",
            "properties": {
                "spark.jars.packages": "io.delta:delta-core_2.13:2.4.0",
            },
        },
    },
    batch_id=batch_name,
    dag=dag
)

