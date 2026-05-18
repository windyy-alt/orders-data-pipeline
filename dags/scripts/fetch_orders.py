import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    print("Fetching data dari Orders API...")
    url = "http://96.9.212.102:8000/orders"
    params = {
        "table_name": "orders",
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        orders = data['orders']

        orders_data = []
        products_data = []
        for order in orders:
            # flatten level orders
            orders_data.append({
                'order_id': order.get('order_id'),
                'user_id': order.get('user_id'),
                'order_number': order.get('order_number'),
                'order_dow': order.get('order_dow'),
                'order_hour_of_day': order.get('order_hour_of_day'),
                'days_since_prior_order': order.get('days_since_prior_order'),
                'eval_set': order.get('eval_set')
            })

            # flatten level products (nested)
            for product in order.get('products', []):
                products_data.append({
                    'order_id': order.get('order_id'),  # fk ke orders
                    'product_id': product.get('product_id'),
                    'product_name': product.get('product_name'),
                    'aisle_id': product.get('aisle_id'),
                    'aisle': product.get('aisle'),
                    'department_id': product.get('department_id'),
                    'department': product.get('department'),
                    'add_to_cart_order': product.get('add_to_cart_order'),
                    'reordered': product.get('reordered')
                })
        
        df_orders = pd.DataFrame(orders_data)
        df_products = pd.DataFrame(products_data)

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        orders_path = f'/opt/airflow/data_lake/orders/orders_{current_time}.parquet'
        products_path = f'/opt/airflow/data_lake/products/products_{current_time}.parquet'
        
        os.makedirs(os.path.dirname(orders_path), exist_ok=True)
        os.makedirs(os.path.dirname(products_path), exist_ok=True)
        
        df_orders.to_parquet(orders_path, index=False)
        df_products.to_parquet(products_path, index=False)
        
        print(f"Successfully fetched and saved {len(df_orders)} orders to {orders_path}")
        print(f"Successfully fetched and saved {len(df_products)} products to {products_path}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

if __name__ == "__main__":
    fetch_orders()