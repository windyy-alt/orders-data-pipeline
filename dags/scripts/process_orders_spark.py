from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    df_orders = spark.read.parquet("file:///opt/airflow/data_lake/orders/orders_*.parquet")
    df_products = spark.read.parquet("file:///opt/airflow/data_lake/products/products_*.parquet")

    df_orders = df_orders.select(
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "eval_set"
    )

    df_products = df_products.select(
        "order_id",
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "add_to_cart_order",
        "reordered"
    )

    df_orders = df_orders.fillna({
        "days_since_prior_order": 0
    })

    print("Mengubah data Spark menjadi tuples...")

    orders_data = [
        tuple(row)
        for row in df_orders.collect()
    ]

    products_data = [
        tuple(row)
        for row in df_products.collect()
    ]

    spark.stop()

    print("Memuat ke ClickHouse Warehouse...")

    # --- PERBAIKAN MULAI DI SINI ---
    # Tambahkan parameter user dan password sesuai dengan pengaturan ClickHouse Anda
    # Jika Anda menggunakan default bawaan docker, biasanya user='default' dan password='' (kosong)
    # ATAU jika Anda mengatur password di docker-compose.yml, masukkan di sini.
    client = Client(
        host='clickhouse-server',
        user='admin',          
        password='rahasia'
    )
    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # DDL tabel orders
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders (
            order_id Int32,
            user_id Int32,
            order_number Int32,
            order_dow Int32,
            order_hour_of_day Int32,
            days_since_prior_order Float64,
            eval_set String
        ) ENGINE = MergeTree()
        ORDER BY order_id
    ''')

    # DDL tabel products
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_products (
            order_id Int32,
            product_id Int32,
            product_name String,
            aisle_id Int32,
            aisle String,
            department_id Int32,
            department String,
            add_to_cart_order Int32,
            reordered Int8
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id )
    ''')


    
    # Mode Overwrite (Truncate & Insert) agar dasbor Metabase selalu fresh
    # API bakal ditaruh di data lake baru ke cllickhouse, baru nanti buat metabase connect ke clickhouse untuk visualisasi
    # Truncate & Insert orders
    client.execute('TRUNCATE TABLE analytics.orders') 
    if orders_data: 
        client.execute('INSERT INTO analytics.orders VALUES', orders_data)



    # Truncate & Insert products
    client.execute('TRUNCATE TABLE analytics.orders_products')
    if products_data:
        client.execute('INSERT INTO analytics.orders_products VALUES', products_data)

    # Menghapus file .parquet yang sudah diproses agar tidak menumpuk
    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")

    
    files = glob.glob('/opt/airflow/data_lake/products/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
    
    print("✅ Pipeline Selesai!")

if __name__ == "__main__":
    run_spark_analytics()