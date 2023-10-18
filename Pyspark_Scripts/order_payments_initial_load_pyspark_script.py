from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

spark = SparkSession \
  .builder \
  .appName("OrderPaymentsCloudSQLToDeltaTable") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
  .getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DecimalType(10, 5), True)
])

db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "dbtable": "order_payments",
    "user": "root",
    "password": "#####"
}

delta_table_path = "gs://ecommerce_de_project_97/DeltaTables/order_payments/"


orderitems_df = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .schema(schema) \
    .load()

orderitems_df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

orderitems_df.show()

spark.stop()

