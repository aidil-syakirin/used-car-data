import pandas as pd
import datetime
import time
import os
from jobs.myvi_utils import page_number, main_extraction, upload_to_datalake

def main(start,end):
    page_list = page_number(start,end)
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

start_page = int(input("Enter the starting page: "))
end_page = int(input("Enter the last page: "))
date = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
local_file_path = f'C:/Users/asyak/Downloads/Page{start_page}to{end_page}({date}).csv'

df = main(start_page,end_page)
df.to_csv(local_file_path, index = False)
print("Data has been succesfully scrapped and saved to CSV file")

# Upload to Azure Data Lake
container_name = "myviseken-data-lake"  # Replace with your container name
directory_path = "raw/carlist"     # Replace with your desired directory structure

# Verify the local file exists before upload
if os.path.exists(local_file_path):
    print(f"File exists at: {local_file_path}")
    upload_to_datalake(local_file_path, container_name, directory_path)
else:
    print(f"File not found at: {local_file_path}")