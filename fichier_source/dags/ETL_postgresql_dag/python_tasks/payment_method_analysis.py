from airflow.providers.postgres.hooks.postgres import PostgresHook

def staging_payment_method_analysis():
    postgres_hook = PostgresHook(postgres_conn_id='postgresql_connection')

    postgres_hook.run("""
        DROP TABLE IF EXISTS payment_method_analysis;

        CREATE TABLE payment_method_analysis AS
        SELECT
            payment_method,
            COUNT(DISTINCT transaction_id) AS total_transactions,
            SUM(total_amount) AS total_sales_amount,
            AVG(discount_applied) AS average_discount,
            SUM(CASE WHEN return_status = 'Returned' THEN 1 ELSE 0 END)::FLOAT
                / COUNT(DISTINCT transaction_id) AS return_rate,
            AVG(total_quantity_sold) AS average_items_per_transaction
        FROM exploded_products
        GROUP BY payment_method;
    """)

if __name__ == "__main__": 
    staging_payment_method_analysis()