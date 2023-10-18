from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession \
  .builder \
  .appName("GeolocationCloudSQLToDeltaTable") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
  .getOrCreate()

schema = StructType([
    StructField("geolocation_zip_code_prefix", IntegerType(), True),
    StructField("geolocation_lat",  StringType(), True),
    StructField("geolocation_lng",  StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True)
])

db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "dbtable": "geolocation",
    "user": "root",
    "password": "#####"
}

delta_table_path = "gs://ecommerce_de_project_97/DeltaTables/geolocation/"

geolocation_df = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .schema(schema) \
    .load()


geolocation_df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

geolocation_df.show()

spark.stop()
