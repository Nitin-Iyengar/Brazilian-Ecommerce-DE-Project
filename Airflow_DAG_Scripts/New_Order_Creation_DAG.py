from io import StringIO
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

default_args = {
    'owner': 'Airflow',
    'start_date': pendulum.datetime(2023, 10, 12, tz="Asia/Kolkata"),
    'catchup': False,
}

dag = DAG(
    'dag_for_orders_data_simulation',
    default_args=default_args,
    schedule_interval='10 12 * * *',
    description='DAG for running data simulation script for new orders,order_items and order payments')


def generate_new_orders():

    airflow_conn_id = "mysql_conn"

    mysql_hook = MySqlHook(mysql_conn_id=airflow_conn_id)

    # Fetch existing customers data from the database
    existing_customers_query = "SELECT customer_id FROM customers"
    existing_customers = mysql_hook.get_records(sql=existing_customers_query)

    # Fetch existing orders data from the database
    existing_orders_query = "SELECT order_purchase_timestamp FROM orders"
    existing_orders = mysql_hook.get_records(sql=existing_orders_query)

    column_names_customers = ["customer_id"]
    column_names_orders = ["order_purchase_timestamp"]

    existing_customers_data = pd.DataFrame(existing_customers, columns=column_names_customers)
    existing_orders_data = pd.DataFrame(existing_orders, columns=column_names_orders)

    max_existing_purchase_time = pd.to_datetime(existing_orders_data["order_purchase_timestamp"],
                                                format="%Y-%m-%d %H:%M:%S").max()

    # Define time intervals for each date column in minutes
    time_intervals = {
        "order_approved_at": {"min": 60, "max": 4320},  # 1 to 3 days
        "order_delivered_carrier_date": {"min": 120, "max": 10080},  # 2 hours to 1 week
        "order_delivered_customer_date": {"min": 1440, "max": 21600},  # 1 day to 15 days
        "order_estimated_delivery_date": {"min": 0, "max": 43200}  # 0 to 30 days
    }

    # Generate new orders with random timestamps and random existing customers
    new_orders = []
    for _ in range(5):  # Generate 5 new orders
        random_purchase_interval = random.randint(10, 180)  # 10 minutes to 3 hours
        random_purchase_timestamp = max_existing_purchase_time + timedelta(minutes=random_purchase_interval)

        new_order = {
            "order_id": fake.uuid4(),
            "customer_id": random.choice(existing_customers_data["customer_id"]),
            "order_purchase_timestamp": random_purchase_timestamp,
        }

        for date_column, interval in time_intervals.items():
            random_interval = random.randint(interval["min"], interval["max"])
            new_order[date_column] = random_purchase_timestamp + timedelta(minutes=random_interval)

        new_orders.append(new_order)

    # Determine order status based on date columns
    for order in new_orders:
        order_status = None
        if order["order_delivered_customer_date"] > datetime.now():
            order_status = "processing"
            order["order_delivered_carrier_date"] = None
            order["order_estimated_delivery_date"] = None
        elif order["order_delivered_customer_date"]:
            order_status = "delivered"
        elif order["order_delivered_carrier_date"]:
            order_status = "shipped"
        elif order["order_approved_at"]:
            order_status = "approved"
        else:
            order_status = "created"

        order["order_status"] = order_status

    # Insert new orders into the orders table
    for new_ord in new_orders:
        insert_new_orders_query = "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        new_order_values = (
            new_ord["order_id"], new_ord["customer_id"], new_ord["order_status"], new_ord["order_purchase_timestamp"],
            new_ord["order_approved_at"], new_ord["order_delivered_carrier_date"],
            new_ord["order_delivered_customer_date"],
            new_ord["order_estimated_delivery_date"])
        mysql_hook.run(sql=insert_new_orders_query, parameters=new_order_values)

    existing_order_items_query = "SELECT product_id,seller_id,price,freight_value FROM order_items"
    existing_order_itm = mysql_hook.get_records(sql=existing_order_items_query)

    colums_names_order_items = ["product_id", "seller_id", "price", "freight_value"]
    existing_order_items = pd.DataFrame(existing_order_itm, columns=colums_names_order_items)

    # Convert order_estimated_delivery_date to datetime
    for order in new_orders:
        order["order_estimated_delivery_date"] = pd.to_datetime(order["order_estimated_delivery_date"])

    # Generate new order items with random prices and freight values
    new_order_items = []

    for new_order in new_orders:
        order_id = new_order["order_id"]
        order_estimated_delivery_date = order["order_estimated_delivery_date"]

        # Determine the number of products in the order (between 1 and 5)
        num_products = random.randint(1, 5)

        for order_item_id in range(1, num_products + 1):
            # Select a random product-seller combination from existing order items
            random_order_item = existing_order_items.sample()
            product_id = random_order_item["product_id"].values[0]
            seller_id = random_order_item["seller_id"].values[0]

            # Calculate the shipping limit date (1 day before the estimated delivery date)
            shipping_limit_date = order_estimated_delivery_date - timedelta(days=1)

            # Create the new order item
            new_order_item = {
                "order_id": order_id,
                "order_item_id": order_item_id,
                "product_id": product_id,
                "seller_id": seller_id,
                "shipping_limit_date": shipping_limit_date,
                "price": round(random.uniform(0.8, float(random_order_item["price"].values[0])), 2),
                "freight_value": round(random.uniform(0, float(random_order_item["freight_value"].values[0])), 2)
            }
            new_order_items.append(new_order_item)

    # Insert new orders items into the order_items table
    for new_ord_itm in new_order_items:
        insert_new_order_items_query = "INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date,price,freight_value) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        new_order_items_values = (
            new_ord_itm["order_id"], new_ord_itm["order_item_id"], new_ord_itm["product_id"], new_ord_itm["seller_id"],
            new_ord_itm["shipping_limit_date"], new_ord_itm["price"], new_ord_itm["freight_value"])
        mysql_hook.run(sql=insert_new_order_items_query, parameters=new_order_items_values)

    # Generate new order payments based on the new order items

    new_order_payments = []
    payment_sequence = 1

    # existing_order_payment_query = "SELECT * FROM order_payments"
    # cursor.execute(existing_order_payment_query)
    # existing_order_payments = cursor.fetchall()

    voucher_payment_percentage = random.uniform(0.2, 0.3)  # Random voucher payment percentage between 20% and 30%

    for order in new_orders:
        order_id = order["order_id"]
        payment_value = sum(
            new_order_item["price"] + new_order_item["freight_value"] for new_order_item in new_order_items if
            new_order_item["order_id"] == order_id)
        payment_installments = random.randint(1, 12)  # Random installment between 1 and 12

        payment_type = random.choice(["boleto", "credit_card", "voucher"])

        if payment_type == "boleto":
            payment_sequence = 1
        elif payment_type == "credit_card":
            payment_sequence = 1
        else:  # voucher
            voucher_amount = round(payment_value * voucher_payment_percentage, 2)
            remaining_amount = payment_value - voucher_amount

            voucher_payment = {
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": "voucher",
                "payment_installments": 1,
                "payment_value": voucher_amount
            }
            new_order_payments.append(voucher_payment)

            credit_card_payment = {
                "order_id": order_id,
                "payment_sequential": 2,
                "payment_type": "credit_card",
                "payment_installments": payment_installments,
                "payment_value": round(remaining_amount, 2)
            }
            new_order_payments.append(credit_card_payment)

        if payment_type != "voucher":
            payment_record = {
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": payment_type,
                "payment_installments": payment_installments,
                "payment_value": round(payment_value, 2)
            }
            new_order_payments.append(payment_record)

    # Insert new orders items into the order_payments table
    for new_ord_pay in new_order_payments:
        insert_new_order_payment_query = "INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) VALUES (%s, %s, %s, %s, %s)"
        new_order_payment_values = (
        new_ord_pay["order_id"], new_ord_pay["payment_sequential"], new_ord_pay["payment_type"],
        new_ord_pay["payment_installments"], new_ord_pay["payment_value"])
        mysql_hook.run(sql=insert_new_order_payment_query, parameters=new_order_payment_values)


order_creation_task = PythonOperator(
    task_id='new_order_creation',
    python_callable=generate_new_orders,
    dag=dag
)

order_creation_task




