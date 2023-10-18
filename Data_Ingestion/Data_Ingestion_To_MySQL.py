import pandas as pd
from io import BytesIO
import pymysql
import openpyxl
import os
from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import storage


# Load your service account credentials
SERVICE_ACCOUNT_KEY_FILE = "D:/Service Accounts/Cloud SQL/sql_admin.json"
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_KEY_FILE,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Build the SQL Admin API service
sqladmin = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

# Initialize a client to interact with Google Cloud Storage
storage_client = storage.Client(credentials=credentials)

db_credentials = {
    "user": "root",
    "password": "#####",
    "host": "34.135.240.44",
    "database": "ecommerce_datasets"
}

# Define your GCS bucket
bucket_name = "ecommerce_de_project_97"

# Table-specific configurations
table_configs = [
    {
        "table_name": "customers",
        "config_uri": "raw_ecommerce_datasets/olist_customers_dataset.xlsx"
    },
    {
        "table_name": "geolocation",
        "config_uri": "raw_ecommerce_datasets/olist_geolocation_dataset.xlsx"
    },
    {
        "table_name": "products",
        "config_uri": "raw_ecommerce_datasets/olist_products_dataset.xlsx"
    },
    {
        "table_name": "sellers",
        "config_uri": "raw_ecommerce_datasets/olist_sellers_dataset.xlsx"
    },
    {
        "table_name": "product_category_name_translation",
        "config_uri": "raw_ecommerce_datasets/product_category_name_translation.xlsx"
    },
    {
        "table_name": "orders",
        "config_uri": "raw_ecommerce_datasets/olist_orders_dataset.xlsx"
    },
    {
        "table_name": "order_items",
        "config_uri": "raw_ecommerce_datasets/olist_order_items_dataset.xlsx"
    },
    {
        "table_name": "order_payments",
        "config_uri": "raw_ecommerce_datasets/olist_order_payments_dataset.xlsx"
    },
]

# Connect to the MySQL database
connection = pymysql.connect(**db_credentials)


# Iterate over each table config and import data
for config in table_configs:
    table_name = config["table_name"]
    config_uri = config["config_uri"]

    # Fetch Excel file from GCS (change the file extension)
    excel_path = os.path.join(bucket_name, config_uri)
    blob = storage_client.bucket(bucket_name).blob(config_uri)
    excel_data = blob.download_as_bytes()  # Download as bytes

    # Read Excel data into a DataFrame using pandas
    data = pd.read_excel(BytesIO(excel_data), engine='openpyxl')

    # Prepare the query and values for insert
    columns = ", ".join(data.columns)
    placeholders = ", ".join(["%s"] * len(data.columns))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    with connection.cursor() as cursor:
        batch_size = 1000  # Adjust the batch size as needed
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            values = [tuple(row) for row in batch.values]
            cursor.executemany(query, values)
            connection.commit()

# Close the database connection
connection.close()

print("All data imported successfully.")
