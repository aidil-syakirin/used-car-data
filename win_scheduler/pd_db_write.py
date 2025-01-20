from dotenv import load_dotenv
import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import io
import pandas as pd
from sqlalchemy.engine import create_engine
from datetime import datetime

#get db connection with sql alchelmy
def get_db_connection():
    load_dotenv()
    #LOAD CREDENTIAL FROM .ENV FILE
    server_name = os.getenv("SQL_SERVER")
    database_name = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    # Connection string using SQL login
    connection_string = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server_name}.database.windows.net:1433/{database_name}"
        "?driver=ODBC+Driver+18+for+SQL+Server&encrypt=yes&TrustServerCertificate=no&timeout=30"
    )

    # Create the SQLAlchemy engine and connect
    engine = create_engine(connection_string)
    return engine

def insert_data(df):
    engine = get_db_connection()

    # Add processing date column to DataFrame
    df['processing_date'] = datetime.now()

    with engine.connect() as connection:
        df.to_sql('test_staging', engine, if_exists='append', index=False)
        
        connection.commit()


load_dotenv()

# Get sensitive data from environment variable
directory_path = os.getenv('AZURE_DIRECTORY_PATH')
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
adls_client_id = os.getenv("ADLS_CLIENT_ID")
adls_client_secret = os.getenv("ADLS_CLIENT_SECRET")
adls_tenant_id = os.getenv("ADLS_TENANT_ID")
conn_string_value = os.getenv('AZURE_DATA_LAKE_CONNECTION_STRING')
container_name = os.getenv('AZURE_CONTAINER_NAME')

# Test credentials
credential = ClientSecretCredential(
    tenant_id= adls_tenant_id,
    client_id= adls_client_id,
    client_secret= adls_client_secret
)

# Test connection
service_client = BlobServiceClient(
    account_url="https://myvisekendatalake.blob.core.windows.net",
    credential=credential
)

# Get container client
container_client = blob_service_client.get_container_client(container_name)

# List all blobs in the specified directory
blob_list = container_client.list_blobs(name_starts_with=directory_path)

# Get list of files
files = [blob.name for blob in container_client.list_blobs(name_starts_with=directory_path)
         if blob.name.lower().endswith(('.csv', '.parquet'))] # filter for .csv or .parquet file only

for blob_name in files:

    blob_service_client = BlobServiceClient.from_connection_string(conn_string_value)# Access the container and blob

    processing_date = pd.Timestamp.now().strftime('%Y%m%d')
    
    if processing_date in blob_name:
        blob_service_client = BlobServiceClient.from_connection_string(conn_string_value)

    # Access the container and blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download the blob content
    stream = blob_client.download_blob().readall()

    # Load into pandas DataFrame
    df = pd.read_parquet(io.BytesIO(stream))

    #preprocessing the raw .parquet file to fit the staging table schema
    
    #add new listing_image column to fit the existing schema
    df['listing_image'] = df['image'].str[0]
    
    #the state column cannot be null as per schema
    df = df[df['state'].isnull() == False]

    #rename model column into car_model column
    df.rename(columns={'model': 'car_model'}, inplace=True)
    
    del df['seller']

    insert_data(df)
    




