from airflow.decorators import dag, task
from datetime import datetime
import pendulum
import pandas as pd
from bs4 import BeautifulSoup as bs
from airflow.providers.microsoft.azure.operators.adls import ADLSCreateObjectOperator
from airflow import DAG
from dotenv import load_dotenv
import requests
import json
import time
import re
import os
import io

from dotenv import load_dotenv
load_dotenv() # take environment variables from .env only for local testing


def page_number(start, end, base_url):
    list_page = []
    for i in range(start, end + 1):
        list_page.append(base_url.replace('page_number=X', f'page_number={i}'))

    return list_page

def extract_url(text):
    # Pattern for URL
    url_pattern = r'(https://www\.carlist\.my/[\w-]+/[^\s]+?)\.?(?=\s|$)'
    
    # Extract URL
    url_match = re.search(url_pattern, text)
    url = url_match.group(1) if url_match else None
    
    return url

def extract_json_file(indv_url):
    
    article1_data = {}
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}
        webpage = requests.get(url = indv_url, headers= headers)
        soup2 = bs(webpage.text, 'html.parser')
        article1 = soup2.find('script', {'type': 'application/ld+json'})
                
        if article1 and article1.string: 
            try: 
                json_data = json.loads(article1.string)
                if isinstance(json_data,list) and len(json_data) > 0:
                    try: 
                        json_data = json_data[0]
                        article1_data.update({ 
                            'model': json_data.get('model'),
                            'title': json_data.get('name'),
                            'year': json_data.get('vehicleModelDate'),
                            'color': json_data.get('color'),
                            'mileage': json_data['mileageFromOdometer'].get('value'),
                            'price': json_data['offers'].get('price'),
                            'seller': json_data['offers']['seller'].get('@type'),
                            'location': json_data['offers']['seller']['homeLocation']['address'].get('addressLocality'),
                            'state': json_data['offers']['seller']['homeLocation']['address'].get('addressRegion'),
                            'url': indv_url,
                            'image': json_data.get('image')
                            })
                        
                    except (KeyError, TypeError) as e:
                        print(f'Error accessing JSON: {e}')
                return article1_data
            except json.JSONDecodeError as e:
                print(f'Error parsing JSON: {e}')
        print('u')
        #time.sleep(2)
        return article1_data
    
    except:
        print(f'Error in extract_json_file: {e}')
        return article1_data
    
def main_extraction(page_url):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}
    page = requests.get(url = page_url, headers=headers)
    soup = bs(page.text, 'html.parser')

    articles = soup.findAll('article',{
            'class': ['js--listing', 'article--details']
        })

    if articles:

        json_results = []

        for article in articles:
            try:
                default_text = article.get('data-default-line-text', '')
                url = extract_url(default_text)
                article_data = extract_json_file(url)
                article_data['listing_id'] = article.get('data-listing-id')
                article_data['installment'] =  article.get('data-installment')
                article_data['variant'] = article.get('data-variant', '')
                article_data['transmission'] = article.get('data-transmission', '')
                if article_data:
                    json_results.append(article_data)
                
            except Exception as e:
                print(f"Error processing article {article.get('data-listing-id', 'unknown')}: {str(e)}")
                continue  # Skip this article and continue with next one
        
        return json_results
    return []

def get_model_name(url):
    if 'myvi' in url.lower():
        return 'myvi'
    elif 'city' in url.lower():
        return 'city'
    elif 'vios' in url.lower():
        return 'vios'
    return 'unknown'

def upload_to_datalake(parquet_buffer): 

    try:

        # Load environment variables from .env file
        load_dotenv()

        container_name = os.getenv('AZURE_CONTAINER_NAME')
        directory_path = os.getenv('AZURE_DIRECTORY_PATH')
        connection_string = os.getenv('AZURE_DATA_LAKE_CONNECTION_STRING')
        
        # Get connection string and other parameters from environment variables
        connection_string = os.getenv('AZURE_DATA_LAKE_CONNECTION_STRING')
        if connection_string is None:
            raise ValueError("Connection string not found")
        
        # Create a DataLakeServiceClient
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        
        # Ensure paths don't have leading/trailing slashes
        directory_path = directory_path.strip('/')
        container_name = container_name.strip()
        
        # Get file system client (container)
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Get directory client
        directory_client = file_system_client.get_directory_client(directory_path)
        
        # Generate a unique file name (you can customize this as needed)
        file_name = f"data_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
        
        # Debug print
        print(f"Attempting to upload file: {file_name}")
        
        # Create directory if it doesn't exist (removed exist_ok parameter)
        try:
            directory_client.create_directory()
        except Exception as e:
            # Directory might already exist, continue
            print(f"Directory might already exist: {str(e)}")
        
        # Get file client
        file_client = directory_client.get_file_client(file_name)
        
        # Upload the parquet data from the buffer
        file_client.upload_data(parquet_buffer.getvalue(), overwrite=True)
        
        print(f"File uploaded successfully to Azure Data Lake: {directory_path}/{file_name}")
        
    except Exception as e:
        print(f"Error uploading to Data Lake: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        print(traceback.format_exc())

@dag(
    start_date=pendulum.datetime(2025,2,18),
    schedule="5 1 * * *",
    catchup=False,
    max_active_runs=1
)
def used_car_data_elt():
   
    @task()
    def extract_data():
        
        base_url = ['https://www.carlist.my/cars-for-sale/perodua/myvi/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc', \
                    'https://www.carlist.my/cars-for-sale/honda/city/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc', \
                    'https://www.carlist.my/cars-for-sale/toyota/vios/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc' ]

        total_run_json_data = []

        for x in base_url:
            all_json_data = []
            model_name = get_model_name(x)
            page_list = page_number(1,1,x)
            # print(page_list)
            try:
                for index,item in enumerate(page_list):
                    results = main_extraction(item)
                    if results:
                        all_json_data.extend(results)
                        print(f"Added {len(results)} entries from page {index + 1}")
                    else:
                        print(f"Processed item {index + 1}: No entries found")

                
            except Exception as e:
                print(f"Error in main processing: {str(e)}")

            total_run_json_data.extend(all_json_data)
        
        if total_run_json_data:
            # Convert to DataFrame
            df = pd.DataFrame(total_run_json_data)

            return df
       
    @task
    def load_data(df):
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow')
        print("Data has been successfully scraped and saved to Parquet format")

        try:
            upload_to_datalake(parquet_buffer)
        except Exception as e:
            print(f"Error uploading to Data Lake: {str(e)}")
            # Print full error traceback for debugging
            import traceback
            print(traceback.format_exc())

        total_run_json_data.extend(all_json_data)
    
    if total_run_json_data:
        df = pd.DataFrame(total_run_json_data)
        return df.to_dict('records')  # Convert DataFrame to dict for XCom

@task(task_id="prepare_and_load_data")
def load_data(data):
    df = pd.DataFrame(data)  # Convert dict back to DataFrame
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')
    parquet_buffer.seek(0)
    print("Data has been successfully scraped and saved to Parquet format")

    # Create the operator directly in the DAG context
    upload_to_adls = ADLSCreateObjectOperator(
        task_id="upload_data_to_adls",
        azure_data_lake_conn_id=azure_connection_id,
        file_system_name=azure_container_name,
        file_name=f"raw/carlist/data_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet",
        data=parquet_buffer.getvalue(),
        replace=True,
        dag=dag  # Add the DAG reference
    )
    
    # Execute the upload
    upload_to_adls.execute(context={})


# Allow the DAG to be run
used_car_data_elt()