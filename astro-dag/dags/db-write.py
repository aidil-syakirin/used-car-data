from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import pendulum
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import io
import os
from sqlalchemy.engine import create_engine
import msal
import pyodbc
import struct
import socket
from sqlalchemy import event
from sqlalchemy.engine import URL
import urllib.parse
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    'adls_to_sql_pipeline',
    default_args=default_args,
    description='Process ADLS data and write to Azure SQL',
    schedule_interval='0 1 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['adls', 'azure', 'sql'],
) as dag:
    
    @task
    def get_env_vars():
        # Get variables from Airflow Variables
        env_vars = {
            "AZURE_TENANT_ID": Variable.get("AZURE_TENANT_ID", default_var=""),
            "ADLS_STORAGE_ACCOUNT": Variable.get("ADLS_STORAGE_ACCOUNT", default_var=""),
            "ADLS_DIRECTORY_PATH": Variable.get("ADLS_DIRECTORY_PATH", default_var=""),
            "ADLS_CONNECTION_STRING": Variable.get("ADLS_CONNECTION_STRING", default_var=""),
            "ADLS_CONTAINER_NAME": Variable.get("ADLS_CONTAINER_NAME", default_var=""),
            "ADLS_CLIENT_ID": Variable.get("ADLS_CLIENT_ID", default_var=""),
            "ADLS_CLIENT_SECRET": Variable.get("ADLS_CLIENT_SECRET", default_var=""),
            "SQL_SERVER": Variable.get("AZSQL_SQL_SERVER", default_var=""),
            "DB_NAME": Variable.get("AZSQL_SQL_DB", default_var=""),
            "DB_USERNAME": Variable.get("AZSQL_DB_USERNAME", default_var=""),
            "DB_PASSWORD": Variable.get("AZQL_DB_PASSWORD", default_var=""),
            "AZSQL_CLIENT_ID": Variable.get("AZSQL_CLIENT_ID", default_var=""),
            "AZSQL_CLIENT_SECRET": Variable.get("AZSQL_CLIENT_SECRET", default_var="")
        }
        
        # Print debug info
        for key, value in env_vars.items():
            if key in ["ADLS_CONNECTION_STRING", "ADLS_CLIENT_SECRET", "DB_PASSWORD", "AZSQL_CLIENT_SECRET"]:
                print(f"{key}: {'*' * 10 if value else 'NOT SET'}")
            else:
                print(f"{key}: {value}")
        
        return env_vars
    
    @task
    def get_single_blob(env_vars):
        tenant_id = env_vars["AZURE_TENANT_ID"].strip()
        client_id = env_vars["ADLS_CLIENT_ID"].strip()
        client_secret = env_vars["ADLS_CLIENT_SECRET"].strip()
        storage_account = env_vars["ADLS_STORAGE_ACCOUNT"].strip()
        container_name = env_vars["ADLS_CONTAINER_NAME"].strip()
        directory_path = env_vars["ADLS_DIRECTORY_PATH"].strip()
        
        # For debugging
        print(f"Using tenant_id: {tenant_id}")
        print(f"Using client_id: {client_id}")
        print(f"Using storage_account: {storage_account}")
        print(f"Using container_name: {container_name}")
        print(f"Using directory_path: {directory_path}")
        
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=credential
        )

        container_client = service_client.get_container_client(container_name)
        
        # Get a single parquet file with the specified date
        processing_date = '20250314'
        
        # Simplified to get just one file
        for blob in container_client.list_blobs(name_starts_with=directory_path):
            if blob.name.lower().endswith('.parquet') and processing_date in blob.name:
                return blob.name
                
        # If no file found
        raise ValueError(f"No parquet file found with date {processing_date}")
    
    @task
    def process_and_load(blob_name, env_vars):
        connection_string = env_vars["ADLS_CONNECTION_STRING"].strip()
        container_name = env_vars["ADLS_CONTAINER_NAME"].strip()
        
        # Download blob
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        stream = blob_client.download_blob().readall()
        
        # Process data
        df = pd.read_parquet(io.BytesIO(stream))
        df['listing_image'] = df['image'].str[0]
        df = df[df['state'].isnull() == False]
        df.rename(columns={'model': 'car_model'}, inplace=True)
        if 'seller' in df.columns:
            del df['seller']
        df['processing_date'] = datetime.now()
        
        # Get SQL credentials
        sql_server = env_vars["SQL_SERVER"].strip()
        db_name = env_vars["DB_NAME"].strip() 
        # username = env_vars["DB_USERNAME"].strip()
        # password = env_vars["DB_PASSWORD"].strip()
        tenant_id = env_vars["AZURE_TENANT_ID"].strip()
        client_id = env_vars["AZSQL_CLIENT_ID"].strip()
        client_secret = env_vars["AZSQL_CLIENT_SECRET"].strip()

        print("Getting MSAL token...")
        # Get token
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        
        token_response = app.acquire_token_for_client(
            scopes=["https://database.windows.net/.default"]
        )
        
        if "access_token" not in token_response:
            raise Exception(f"Token acquisition failed: {token_response.get('error_description')}")

        access_token = token_response["access_token"]
        token_bytes = bytes(access_token, "utf-8")
        ex_token = struct.pack(f"{len(token_bytes)}s", token_bytes)

        # Create connection string
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{sql_server}.database.windows.net,1433;"
            f"Database={db_name};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        print("Connecting to database...")
        try:
            # Use pyodbc for the connection
            conn = pyodbc.connect(
                conn_str,
                attrs_before={1256: ex_token},
                autocommit=True
            )
            
            # Add these before the connection attempt
            print(f"Connection string: {conn_str}")
            print(f"Token length: {len(access_token)}")
            print(f"Using SQL Server: {sql_server}")
            print(f"Using Database: {db_name}")

            # Test connection immediately after establishing it
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            print(f"Connected successfully! SQL Version: {version[0]}")
            
            # Process data in chunks
            chunk_size = 1000
            for i in range(0, len(df), chunk_size):
                chunk = df[i:i + chunk_size]
                
                # Convert chunk to list of tuples
                columns = ','.join(df.columns)
                placeholders = ','.join(['?' for _ in df.columns])
                records = chunk.to_records(index=False)
                
                # Insert using pyodbc
                cursor = conn.cursor()
                cursor.fast_executemany = True
                insert_sql = f"INSERT INTO test_staging ({columns}) VALUES ({placeholders})"
                cursor.executemany(insert_sql, records)
                conn.commit()
                
                print(f"Inserted chunk {i//chunk_size + 1}")
                
        except Exception as e:
            print(f"Error: {str(e)}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    @task
    def test_quick_connection(env_vars):
        """Test connection with minimal timeout"""
        sql_server = env_vars["SQL_SERVER"].strip()
        server = f"{sql_server}.database.windows.net"
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((server, 1433))
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
        finally:
            sock.close()

    # Define the task execution
    env_vars_task = get_env_vars()
    quick_test = test_quick_connection(env_vars_task)
    blob_name_task = get_single_blob(env_vars_task)
    result_task = process_and_load(blob_name_task, env_vars_task)
    
    # Set the dependencies
    env_vars_task >> quick_test >> blob_name_task >> result_task