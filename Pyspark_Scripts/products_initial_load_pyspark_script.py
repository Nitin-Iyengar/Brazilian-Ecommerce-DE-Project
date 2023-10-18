from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, coalesce

spark = SparkSession \
  .builder \
  .appName("ProductsCloudSQLToDeltaTable") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
  .getOrCreate()

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True)
])

sql_query = """
    (SELECT p.product_id, p.product_category_name, pt.product_category_name_english
    FROM products p
    LEFT JOIN
    product_category_name_translation pt
    ON p.product_category_name = pt.product_category_name ) AS products_subset"""


db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "dbtable": sql_query,
    "user": "root",
    "password": "#####"
}

delta_table_path = "gs://ecommerce_de_project_97/DeltaTables/products/"

products_df = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .schema(schema) \
    .load()


products_df = products_df.withColumn("product_category_in_english", coalesce(col("product_category_name_english"), lit("others")))

products_filter = products_df.select(col("product_id"), col("product_category_name"), col("product_category_in_english"))

products_filter.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

products_filter.show()

spark.stop()
