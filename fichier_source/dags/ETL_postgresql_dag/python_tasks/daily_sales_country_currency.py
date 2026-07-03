from airflow.providers.postgres.hooks.postgres import PostgresHook

def staging_daily_sales_country_currency():
    postgres_hook = PostgresHook(postgres_conn_id='postgresql_connection')

    postgres_hook.run("""
        DROP TABLE IF EXISTS daily_sales_country_currency;

        CREATE TABLE daily_sales_country_currency AS
        SELECT
            DATE(purchase_timestamp) AS purchase_date,
            store_country,
            currency,
            SUM(total_amount) AS daily_total_sales,
            COUNT(DISTINCT transaction_id) AS number_of_transactions,
            AVG(total_amount) AS average_transaction_value
        FROM exploded_products
        GROUP BY DATE(purchase_timestamp), store_country, currency;
    """)

if __name__ == "__main__": 
    staging_daily_sales_country_currency()