from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json
import re
from main import page_number, main_extraction,extract_location_and_date,extract_price_and_url

# Keep the helper functions (page_number, extract_price_and_url, extract_location_and_date, main_extraction)
# ... existing helper functions ...

def scrape_myvi_data(**context):
    """Main function to scrape Myvi data"""
    start_page = 1  # You can parameterize this using Variable or config
    end_page = 10   # You can parameterize this using Variable or config
    
    page_list = page_number(start_page, end_page)
    all_df = pd.DataFrame([])

    try:
        for index, item in enumerate(page_list):
            results = main_extraction(item)
            
            if len(results) > 0:
                all_df = pd.concat([all_df, results])
            else:
                print(f"Processed item {index + 1}: No entries found")
    
    except Exception as e:
        print(f"Error in main processing: {str(e)}")
        return None
    
    # Generate output path with timestamp
    date = context['logical_date'].strftime("%Y%m%d%H%M%S")
    output_path = f'/opt/airflow/data/myvi_data_page{start_page}to{end_page}_{date}.csv'
    
    all_df.to_csv(output_path, index=False)
    return output_path

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'myvi_scraping_dag',
    default_args=default_args,
    description='Daily scraping of Myvi car listings',
    schedule_interval='0 22 * * *',  # 6AM GMT+8 (22:00 UTC previous day)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'myvi'],
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_myvi_data',
        python_callable=scrape_myvi_data,
    )

    # Add more tasks here if needed, such as data validation, transformation, or loading