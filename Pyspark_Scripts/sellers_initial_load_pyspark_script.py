from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession \
  .builder \
  .appName("SellersCloudSQLToDeltaTable") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
  .getOrCreate()

schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "dbtable": "sellers",
    "user": "root",
    "password": "#####"
}

delta_table_path = "gs://ecommerce_de_project_97/DeltaTables/sellers/"


sellers_df = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .schema(schema) \
    .load()

sellers_df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

sellers_df.show()

spark.stop()

