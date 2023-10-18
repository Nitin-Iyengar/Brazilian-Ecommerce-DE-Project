import pymysql

# Cloud SQL instance's public IP address
INSTANCE_IP_ADDRESS = "34.135.240.44"

USER = "root"
PASSWORD = "######"

# Connection to the Cloud SQL instance
connection = pymysql.connect(
    host=INSTANCE_IP_ADDRESS,
    user=USER,
    password=PASSWORD,
    db="",
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with connection.cursor() as cursor:
        # Creating a new database
        create_db_query = "CREATE DATABASE IF NOT EXISTS ecommerce_datasets"
        cursor.execute(create_db_query)

        # Use the new database
        cursor.execute("USE ecommerce_datasets")


        create_table_customers = """
            CREATE TABLE IF NOT EXISTS customers(
            customer_id varchar(50) NOT NULL,
            customer_unique_id varchar(50) NOT NULL,
            customer_zip_code_prefix int,
            customer_city varchar(50),
            customer_state char(5),
            PRIMARY KEY (customer_id),
            UNIQUE INDEX indx_cust(customer_id)
            )
        """
        cursor.execute(create_table_customers)

        create_table_geolocation = """
            CREATE TABLE IF NOT EXISTS geolocation(
            geolocation_zip_code_prefix	int NOT NULL,
            geolocation_lat	varchar(50),
            geolocation_lng	varchar(50),
            geolocation_city varchar(50),
            geolocation_state char(5)
)
                """
        cursor.execute(create_table_geolocation)

        create_table_products = """
            CREATE TABLE IF NOT EXISTS products(
            product_id varchar(50) NOT NULL,
            product_category_name varchar(50),
            product_name_length	int,
            product_description_length	int,
            product_photos_qty int,
            product_weight_g int,
            product_length_cm int,
            product_height_cm int,
            product_width_cm int,
            PRIMARY KEY (product_id),
            UNIQUE INDEX indx_prod(product_id)
)

                """
        cursor.execute(create_table_products)

        create_table_sellers = """
            CREATE TABLE IF NOT EXISTS sellers(
            seller_id varchar(50) NOT NULL,
            seller_zip_code_prefix int,
            seller_city	varchar(50),
            seller_state char(5),
            PRIMARY KEY(seller_id),
            UNIQUE INDEX indx_prod(seller_id)
)
                """
        cursor.execute(create_table_sellers)

        create_table_product_category_name_translation = """
            CREATE TABLE IF NOT EXISTS product_category_name_translation(
            product_category_name varchar(50),
            product_category_name_english varchar(50)
)
                """
        cursor.execute(create_table_product_category_name_translation)

        create_table_orders = """
            CREATE TABLE IF NOT EXISTS orders(
            order_id varchar(50) NOT NULL,
            customer_id	varchar(50) NOT NULL,
            order_status varchar(20),
            order_purchase_timestamp datetime,
            order_approved_at datetime,
            order_delivered_carrier_date datetime,
            order_delivered_customer_date datetime,
            order_estimated_delivery_date datetime, 
            PRIMARY KEY(order_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            UNIQUE INDEX indx_prod(order_id)
)
                """
        cursor.execute(create_table_orders)

        create_table_order_items = """
            CREATE TABLE IF NOT EXISTS order_items(
            order_id varchar(50) NOT NULL,
            order_item_id int,
            product_id	varchar(50) NOT NULL,
            seller_id varchar(50) NOT NULL,
            shipping_limit_date	datetime,
            price decimal(10,5),
            freight_value decimal(10,5),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
)
                """
        cursor.execute(create_table_order_items)

        create_table_query = """
            create table order_payments(
            order_id varchar(50) NOT NULL,
            payment_sequential int,
            payment_type varchar(20),
            payment_installments int,
            payment_value decimal(10,5),
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
)
                """
        cursor.execute(create_table_query)


    connection.commit()

finally:
    connection.close()
