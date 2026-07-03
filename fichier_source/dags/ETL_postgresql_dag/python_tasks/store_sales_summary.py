from airflow.providers.postgres.hooks.postgres import PostgresHook

def staging_store_sales_summary():
    postgres_hook = PostgresHook(postgres_conn_id='postgresql_connection')

    postgres_hook.run("""
        DROP TABLE IF EXISTS store_sales_summary;

        CREATE TABLE store_sales_summary AS
        SELECT
            store_id,
            store_name,
            store_country,
            store_city,
            store_type,
            SUM(total_amount) AS total_sales_amount,
            SUM(total_quantity_sold) AS total_quantity_sold
        FROM exploded_products
        GROUP BY store_id, store_name, store_country, store_city, store_type;
    """)

if __name__ == "__main__": 
    staging_store_sales_summary()