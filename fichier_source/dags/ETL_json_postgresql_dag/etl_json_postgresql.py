import json
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

db_config = {
                    'host': 'fichier_source-postgres_destination_server-1',
                    'port': '5432',
                    'database': 'sports_articles',
                    'user': 'admin',
                    'password': 'admin1234'
                }   



def load_data() :
    json_file_path = "/opt/airflow/data/pos_sales_data_30k.json"

    # PostgreSQL connection
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Open the transformed JSON file for reading
    with open(json_file_path, 'r') as json_file:
              data = json.load(json_file)
              # Extract fields from the JSON record
              for element in data:
                transaction_id = element['transaction_id']
                store_id = element['store_id']
                store_name = element['store_name']
                store_country = element['store_country']
                store_city = element['store_city']
                store_type = element['store_type'] 
                purchase_timestamp = element['purchase_timestamp']
                payment_method = element['payment_method']
                currency = element['currency']
                total_amount = element['total_amount']
                total_quantity_sold = element['total_quantity_sold']
                discount_applied = element['discount_applied']
                return_status = element['return_status']
                #print("Transaction ID : " + transaction_id,"Store ID : " + store_id,"Store Name : " + store_name,"Store Country : " + store_country, "Store City : " + store_city)
                for products in element['products']:
                    product_id = products['product_id']
                    product_name = products['product_name']
                    product_category = products['products_category']
                    quantity = products['quantity']
                    price = products['price']
                    #print("  Product ID : " + product_id," Product Name : " + product_name," Product Category : " + product_category," Quantity : " + str(quantity)," Price : " + str(price))
                #print("Total Quantity Sold : " + str(total_quantity_sold)," Total Amount : " + str(total_amount)," Payment Method : " + payment_method," Discount Applied : " + str(discount_applied)," Store Type : " + store_type," Purchase Timestamp : " + purchase_timestamp," Return Status : " + return_status," Currency : " + currency)

                # Define the PostgreSQL query to insert data into the table
                insert_query = '''
                INSERT INTO exploded_products (transaction_id, store_id, store_name ,store_country, store_city, store_type, purchase_timestamp,payment_method,currency,total_amount ,total_quantity_sold,discount_applied, return_status,product_id,product_name,product_category,quantity,price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)
                '''
                record_line = (transaction_id, store_id, store_name ,store_country, store_city, store_type, purchase_timestamp,payment_method,currency,total_amount ,total_quantity_sold,discount_applied, return_status,product_id,product_name,product_category,quantity,price)
                cur.execute(insert_query, record_line)
    conn.commit()
    cur.close()
    conn.close()

# Define default arguments for the DAG
default_args = {
    'owner': 'Bereket Tafesse',        # Owner of the DAG
    'retries': 0,                  # Number of retries in case of failure (disabled)
    'start_date': datetime(2025, 1, 1),  # Start date of the DAG
    'is_paused_upon_creation': True  # This will pause the DAG upon creation
}
              
# Define the DAG
dag = DAG(
    'ETL_json_postgresql',             # DAG ID
    default_args=default_args,     # Default arguments for the DAG
    description='JSON PostgreSQL ETL process for sports articles sales data',
    tags=['Personal',"Intermediate%+"],
    catchup=False,              # Do not backfill past runs when DAG is created
    schedule='@daily'  # Schedule interval (run at midnight every day)
)

load_data_to_mysql = PythonOperator(
    task_id='load_data_to_mysql',
    python_callable=load_data,
    dag=dag
)

load_data_to_mysql  