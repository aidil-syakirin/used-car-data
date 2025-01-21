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

#   # Create a DataLakeServiceClient
#         service_client = DataLakeServiceClient.from_connection_string(connection_string)
        
#         # Ensure paths don't have leading/trailing slashes
#         directory_path = directory_path.strip('/')
#         container_name = container_name.strip()
        
#         # Get file system client (container)
#         file_system_client = service_client.get_file_system_client(file_system=container_name)
        
#         # Get directory client
#         directory_client = file_system_client.get_directory_client(directory_path)
        
#         # Generate a unique file name (you can customize this as needed)
#         file_name = f"vios_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"
        
#         # Debug print
#         print(f"Attempting to upload file: {file_name}")
        
#         # Create directory if it doesn't exist (removed exist_ok parameter)
#         try:
#             directory_client.create_directory()
#         except Exception as e:
#             # Directory might already exist, continue
#             print(f"Directory might already exist: {str(e)}")
        
#         # Get file client
#         file_client = directory_client.get_file_client(file_name)
        
#         # Upload the CSV data from the buffer
#         file_client.upload_data(csv_buffer.getvalue(), overwrite=True)
        
#         print(f"File uploaded successfully to Azure Data Lake: {directory_path}/{file_name}")
        
#     except Exception as e:
#         print(f"Error uploading to Data Lake: {str(e)}")
#         # Print full error traceback for debugging
#         import traceback
#         print(traceback.format_exc())
