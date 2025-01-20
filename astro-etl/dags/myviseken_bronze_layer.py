from airflow.decorators import dag, task
from datetime import datetime
import pendulum
import pandas as pd
import os
import io
from jobs.myvi_utils import page_number, main_extraction, upload_to_datalake

from dotenv import load_dotenv
load_dotenv() # take environment variables from .env only for local testing


@dag(
    start_date=pendulum.datetime(2024,12,30),
    schedule=None,
    catchup=False,
    max_active_runs=1
)
def myviseken_bronze_etl():
    @task()
    def extract_data():
        def main(start,end):
            #base_url = 'https://www.carlist.my/cars-for-sale/perodua/myvi/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc'
            base_url = 'https://www.carlist.my/cars-for-sale/honda/city/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc'
            # base_url = 'https://www.carlist.my/cars-for-sale/toyota/vios/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc'
            page_list = page_number(start,end,base_url)
            all_df = pd.DataFrame([])

            try:
                for index,item in enumerate(page_list):
                    results = main_extraction(item)
                       
                    if len(results) > 0:
                        all_df = pd.concat([all_df,results])

                    else:
                        print(f"Processed item {index + 1}: No entries found")
            
            except Exception as e:
                print(f"Error in main processing: {str(e)}")
                return pd.DataFrame()
            
            return all_df

        df = main(1,160)
        return df
       
    @task
    def load_data(df):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index = False)
        print("Data has been succesfully scrapped and saved to CSV file")

        try:
            upload_to_datalake(csv_buffer)
        except Exception as e:
            print(f"Error uploading to Data Lake: {str(e)}")
            # Print full error traceback for debugging
            import traceback
            print(traceback.format_exc())

    # Set dependencies using function calls
    df = extract_data()
    load_data(df)


# Allow the DAG to be run
myviseken_bronze_etl()
