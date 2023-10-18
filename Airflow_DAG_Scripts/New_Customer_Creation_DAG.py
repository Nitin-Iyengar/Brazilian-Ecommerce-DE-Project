from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from faker import Faker
import random
import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook
import pendulum

fake = Faker()

# Define your DAG default arguments
default_args = {
    'owner': 'Airflow',
    'start_date': pendulum.datetime(2023, 10, 12, tz="Asia/Kolkata"),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    'dag_for_customers_data_simulation',
    default_args=default_args,
    schedule_interval='0 12 * * *',
    description='DAG for running data simulation script for new customers')

def generate_new_data():

    airflow_conn_id = "mysql_conn"

    mysql_hook = MySqlHook(mysql_conn_id=airflow_conn_id)

    # Fetch existing geolocation data from the database
    existing_geolocation_query = "SELECT geolocation_zip_code_prefix, geolocation_city, geolocation_state FROM geolocation LIMIT 100000"
    existing_geolocations = mysql_hook.get_records(sql=existing_geolocation_query)

    # Define column names
    column_names = ["geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"]

    # Prepare the geolocation data for generating new customers
    geolocation_data = pd.DataFrame(existing_geolocations, columns=column_names)
    print(geolocation_data.columns)

    num_geolocations = len(geolocation_data)

    new_customers = []
    for _ in range(5):
        random_geolocation_index = random.randint(0, num_geolocations - 1)
        random_geolocation = geolocation_data.iloc[random_geolocation_index]

        new_customers.append({
            "customer_id": fake.uuid4(),
            "customer_unique_id": fake.uuid4(),
            "customer_zip_code_prefix": random_geolocation["geolocation_zip_code_prefix"],
            "customer_city": random_geolocation["geolocation_city"],
            "customer_state": random_geolocation["geolocation_state"]
        })

    # Insert new customers into the customers table
    for new_cust in new_customers:
        insert_new_customers_query = "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s)"
        new_customer_values = (new_cust["customer_id"], new_cust["customer_unique_id"], new_cust["customer_zip_code_prefix"], new_cust["customer_city"], new_cust["customer_state"])
        mysql_hook.run(sql=insert_new_customers_query, parameters=new_customer_values)


customer_creation_task = PythonOperator(
    task_id='new_customer_creation',
    python_callable=generate_new_data,
    dag=dag
)

customer_creation_task
