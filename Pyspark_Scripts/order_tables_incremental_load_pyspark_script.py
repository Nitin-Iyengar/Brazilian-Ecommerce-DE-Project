from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder \
    .appName("OrdersIncrementalLoadToDeltaTable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core:2.4.0") \
    .getOrCreate()

delta_table_path = "gs://ecommerce_de_project_97/DeltaTables/orders/"

delta_df = spark.read.format("delta").load(delta_table_path)
max_order_date = delta_df.agg({"order_purchase_timestamp": "max"}).collect()[0][0]

db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://10.150.144.3:3306/ecommerce_datasets",
    "user": "root",
    "password": "#####"
}

new_orders_query = f"""
    SELECT * FROM orders
    WHERE order_purchase_timestamp > '{max_order_date}'
"""

new_orders_df = spark.read \
    .format("jdbc") \
    .options(**db_properties) \
    .option("dbtable", "orders") \
    .load()

new_orders_df = new_orders_df.filter(new_orders_df.order_purchase_timestamp > max_order_date)


if new_orders_df.count() > 0:
    new_orders_df.write.format("delta") \
        .mode("append") \
        .save(delta_table_path)


    order_items_query = f"""
        SELECT * FROM order_items
        WHERE order_id IN (SELECT order_id FROM new_orders)
    """

    order_items_updates = spark.read \
        .format("jdbc") \
        .options(**db_properties) \
        .option("dbtable", f"({order_items_query}) as order_items_updates") \
        .load()

    deltaTableorderitems = DeltaTable.forPath(spark, 'gs://ecommerce_de_project_97/DeltaTables/order_items/')

    deltaTableorderitems.alias('order_items') \
        .merge(
            source=order_items_updates.alias('updates'),
            condition='order_items.order_id = updates.order_id AND order_items.order_item_id = updates.order_item_id'
        ) \
        .whenMatchedUpdate(set=
        {
            "order_id": "updates.order_id",
            "order_item_id": "updates.order_item_id",
            "product_id": "updates.product_id",
            "seller_id": "updates.seller_id",
            "shipping_limit_date": "updates.shipping_limit_date",
            "price": "updates.price",
            "freight_value": "updates.freight_value"
        }
        ) \
        .whenNotMatchedInsert(values=
        {
            "order_id": "updates.order_id",
            "order_item_id": "updates.order_item_id",
            "product_id": "updates.product_id",
            "seller_id": "updates.seller_id",
            "shipping_limit_date": "updates.shipping_limit_date",
            "price": "updates.price",
            "freight_value": "updates.freight_value"
        }
        ) \
        .execute()


    order_payments_query = f"""
            SELECT * FROM order_payments
            WHERE order_id IN (SELECT order_id FROM new_orders)
        """

    order_payments_updates = spark.read \
        .format("jdbc") \
        .options(**db_properties) \
        .option("dbtable", f"({order_payments_query}) as order_payments_updates") \
        .load()

    deltaTableorderpayments = DeltaTable.forPath(spark, 'gs://ecommerce_de_project_97/DeltaTables/order_payments/')

    deltaTableorderpayments.alias('order_payments') \
        .merge(
        source=order_payments_updates.alias('updates'),
        condition='order_payments.order_id = updates.order_id AND order_payments.payment_sequential = updates.payment_sequential'
    ) \
        .whenMatchedUpdate(set=
    {
        "order_id": "updates.order_id",
        "payment_sequential": "updates.payment_sequential",
        "payment_type": "updates.payment_type",
        "payment_installments": "updates.payment_installments",
        "payment_value": "updates.payment_value"
    }
    ) \
        .whenNotMatchedInsert(values=
    {
        "order_id": "updates.order_id",
        "payment_sequential": "updates.payment_sequential",
        "payment_type": "updates.payment_type",
        "payment_installments": "updates.payment_installments",
        "payment_value": "updates.payment_value"
    }
    ) \
        .execute()

else:
    print("No new orders found.")

spark.stop()
