from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder \
    .appName("CustomersIncrementalLoadToDeltaTable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
    .getOrCreate()


# Define database connection properties and query for new orders
db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "dbtable": "customers",
    "user": "root",
    "password": "#####"
}

customer_updates = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .load()


deltaTablecustomers = DeltaTable.forPath(spark, 'gs://ecommerce_de_project_97/DeltaTables/customers/')

deltaTablecustomers.alias('customers') \
    .merge(
        source=customer_updates.alias('updates'),
        condition='customers.customer_id = updates.customer_id'
    ) \
    .whenMatchedUpdate(set=
    {
        "customer_id": "updates.customer_id",
        "customer_unique_id": "updates.customer_unique_id",
        "customer_zip_code_prefix": "updates.customer_zip_code_prefix",
        "customer_city": "updates.customer_city",
        "customer_state": "updates.customer_state",
    }
        ) \
    .whenNotMatchedInsert(values=
    {
        "customer_id": "updates.customer_id",
        "customer_unique_id": "updates.customer_unique_id",
        "customer_zip_code_prefix": "updates.customer_zip_code_prefix",
        "customer_city": "updates.customer_city",
        "customer_state": "updates.customer_state",
    }
    ) \
    .execute()

spark.stop()
