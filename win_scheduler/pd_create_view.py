import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from sqlalchemy.engine import create_engine

#connecting to azure sql database for creating a view for the dasboard
def get_db_connection():
    load_dotenv()
    #LOAD CREDENTIAL FROM .ENV FILE
    server_name = os.getenv("SQL_SERVER")
    database_name = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    # Connection string using SQL login
    asql_connection_string = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server_name}.database.windows.net:1433/{database_name}"
        "?driver=ODBC+Driver+18+for+SQL+Server&encrypt=yes&TrustServerCertificate=no&timeout=30"
    )
    engine = create_engine(asql_connection_string)
    return engine

def create_view():
    engine = get_db_connection()

    with engine.connect() as connection:
       # Query the view
        query = "SELECT * FROM test_staging;"
        df = pd.read_sql(query, connection)
        # Save to CSV
        df.to_parquet("used-car-data-view.parquet", index=False)

        return df
        
# Upload to Azure Blob Storage
load_dotenv()
adls_connection_string = os.getenv('AZURE_DATA_LAKE_CONNECTION_STRING')
if adls_connection_string is None:
            raise ValueError("Connection string not found")
blob_service_client = BlobServiceClient.from_connection_string(adls_connection_string)
df_view = create_view()
df_view = df_view.to_parquet("used-car-data-view.parquet", index=False)

blob_client = blob_service_client.get_blob_client(container="used-car-data-dashboard-view", blob="used-car-data-view.parquet")
with open("used-car-data-view.parquet", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
