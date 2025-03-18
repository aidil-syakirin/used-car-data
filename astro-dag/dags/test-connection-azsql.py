from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="test_mssql_connection",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    # Test Azure SQL Connection
    test_connection = SQLExecuteQueryOperator(
        task_id="test_connection",
        conn_id="azsql_used_car_data",  # Make sure this matches your connection ID
        hook_params={"driver": "ODBC Driver 18 for SQL Server"},
        sql="SELECT 1 from myviseken-db.used_car_data.daily_sink;",
        autocommit=True,
    )

    test_connection
