from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob
import logging
import traceback
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(processName)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_spark_analytics():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_time = datetime.now()
    status = "RUNNING"
    error_message = ""
    total_orders = 0
    total_products = 0

    logger.info(f"Starting pipeline Run ID: {run_id}")

    spark = SparkSession.builder \
        .appName("Orders_Advanced_Analytics") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    try:
        orders_files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")
        products_files = glob.glob("/opt/airflow/data_lake/products/*.parquet")

        if not orders_files or not products_files:
            logger.warning("Parquet files not found in Data Lake. Stopping process.")
            status = "SKIPPED"
            return

        logger.info("Reading all data streams from Data Lake...")
        df_orders = spark.read.parquet("file:///opt/airflow/data_lake/orders/*.parquet")
        df_products = spark.read.parquet("file:///opt/airflow/data_lake/products/*.parquet")

        # Count initial rows for monitoring
        total_orders = df_orders.count()
        total_products = df_products.count()
        
        # Simple Anomaly Detection (Observability)
        if total_orders == 0:
            raise ValueError("Orders data is empty after reading from Parquet.")

        df_orders = df_orders.select(
            "order_id", "user_id", "order_number", "order_dow",
            "order_hour_of_day", "days_since_prior_order", "eval_set"
        ).fillna({"days_since_prior_order": 0})

        df_products = df_products.select(
            "order_id", "product_id", "product_name", "aisle_id",
            "aisle", "department_id", "department", "add_to_cart_order", "reordered"
        )

        logger.info("Calculating RFM & Data Marts...")
        
        df_joined = df_orders.join(df_products, "order_id")

        # RFM Calculation:
        # - Recency (Proxy): Average days between orders (smaller means more frequent/recent)
        # - Frequency: Total unique orders
        # - Monetary (Proxy): Total items (volume) purchased
        df_rfm = df_joined.groupBy("user_id").agg(
            F.countDistinct("order_id").alias("frequency"),
            F.count("product_id").alias("monetary_volume"),
            F.sum("days_since_prior_order").alias("total_days_active")
        ).withColumn(
            "recency_avg_days", 
            F.round(F.col("total_days_active") / F.col("frequency"), 2)
        )

        df_rfm = df_rfm.withColumn(
            "customer_segment",
            F.when(F.col("frequency") >= 10, "Loyal")
             .when((F.col("frequency") >= 4) & (F.col("frequency") < 10), "Regular")
             .otherwise("New/Occasional")
        )

        logger.info("Converting DataFrame to tuple array for ClickHouse...")
        orders_data = [tuple(row) for row in df_orders.collect()]
        products_data = [tuple(row) for row in df_products.collect()]
        rfm_data = [tuple(row) for row in df_rfm.collect()]

        status = "SUCCESS"

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        logger.error(f"Spark Processing Error: {error_message}")
        logger.error(traceback.format_exc())
        raise
    finally:
        spark.stop()
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info("Connecting to ClickHouse Warehouse...")
        try:
            client = Client(
                host=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'),
                user=os.getenv('CLICKHOUSE_USER', 'admin'),          
                password=os.getenv('CLICKHOUSE_PASSWORD', 'rahasia')
            )
            
            client.execute('CREATE DATABASE IF NOT EXISTS analytics')

            # DDL for Raw Tables
            client.execute('''
                CREATE TABLE IF NOT EXISTS analytics.orders (
                    order_id Int32, user_id Int32, order_number Int32, order_dow Int32,
                    order_hour_of_day Int32, days_since_prior_order Float64, eval_set String
                ) ENGINE = MergeTree() ORDER BY order_id
            ''')
            client.execute('''
                CREATE TABLE IF NOT EXISTS analytics.orders_products (
                    order_id Int32, product_id Int32, product_name String, aisle_id Int32,
                    aisle String, department_id Int32, department String, add_to_cart_order Int32, reordered Int8
                ) ENGINE = MergeTree() ORDER BY (order_id, product_id)
            ''')

            # DDL for RFM Mart Table
            client.execute('''
                CREATE TABLE IF NOT EXISTS analytics.mart_rfm (
                    user_id Int32, frequency Int32, monetary_volume Int32,
                    total_days_active Float64, recency_avg_days Float64, customer_segment String
                ) ENGINE = MergeTree() ORDER BY frequency
            ''')

            # DDL for Pipeline Observability Table
            client.execute('''
                CREATE TABLE IF NOT EXISTS analytics.pipeline_logs (
                    run_id String, execution_time DateTime, status String,
                    total_orders_processed Int32, total_products_processed Int32,
                    duration_seconds Float32, error_message String
                ) ENGINE = MergeTree() ORDER BY execution_time
            ''')

            if status == "SUCCESS":
                # Truncate & Insert Raw Data
                client.execute('TRUNCATE TABLE analytics.orders') 
                if orders_data: client.execute('INSERT INTO analytics.orders VALUES', orders_data)

                client.execute('TRUNCATE TABLE analytics.orders_products')
                if products_data: client.execute('INSERT INTO analytics.orders_products VALUES', products_data)

                # Truncate & Insert Mart Data
                client.execute('TRUNCATE TABLE analytics.mart_rfm')
                if rfm_data: client.execute('INSERT INTO analytics.mart_rfm VALUES', rfm_data)
                
                logger.info("Data successfully loaded into ClickHouse.")

            log_tuple = [(run_id, start_time, status, total_orders, total_products, duration_seconds, error_message)]
            client.execute('INSERT INTO analytics.pipeline_logs VALUES', log_tuple)
            logger.info("Pipeline log successfully saved.")

        except Exception as e:
            logger.error(f"Failed to load data into ClickHouse: {e}")
            raise

        if status == "SUCCESS":
            logger.info("Cleaning up old Parquet files from Data Lake...")
            for folder in ['/opt/airflow/data_lake/orders/', '/opt/airflow/data_lake/products/']:
                for f in glob.glob(folder + '*.parquet'):
                    try:
                        os.remove(f)
                    except OSError as e:
                        logger.error(f"Error deleting {f}: {e.strerror}")

        logger.info(f"Pipeline completed with status: {status}")

if __name__ == "__main__":
    run_spark_analytics()