import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json
import datetime
import time
import re
from azure.storage.filedatalake import DataLakeServiceClient
import os
from dotenv import load_dotenv
import io


def page_number(start, end, base_url):
    list_page = []
    for i in range(start, end + 1):
        list_page.append(base_url.replace('page_number=X', f'page_number={i}'))

    return list_page

def extract_price_and_url(text):
 
    # Pattern for RM price
    price_pattern = r'RM\s*([0-9,]+)'
    
    # Pattern for URL
    url_pattern = r'(https://www\.carlist\.my/[\w-]+/[^\s]+?)\.?(?=\s|$)'
    
    # Extract price
    price_match = re.search(price_pattern, text)
    price = price_match.group(1).replace(' ', '') if price_match else None
    
    # Extract URL
    url_match = re.search(url_pattern, text)
    url = url_match.group(1) if url_match else None
    
    return price, url

def extract_location_and_date(indv_url):
    
    article1_data = {
        'location': None,
        'state': None,
        'image': None
    } 
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
                        car_data = json_data[0]["offers"]['seller']['homeLocation']['address']
                        image_data = json_data[0]['image']
                        article1_data.update({ 
                            'location': car_data.get('addressLocality'),
                            'state': car_data.get('addressRegion'),
                            'image': image_data
                            })
                    except (KeyError, TypeError) as e:
                        print(f'Error accessing JSON: {e}')
            except json.JSONDecodeError as e:
                print(f'Error parsing JSON: {e}')
        
        #time.sleep(2)
        return article1_data
    
    except:
        print(f'Error in extract_location_and_date: {e}')
        return article1_data
    
def main_extraction(page_url):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}

    page = requests.get(url = page_url, headers=headers)
    soup = bs(page.text, 'html.parser')

    articles = soup.findAll('article',{
            'class': ['js--listing', 'article--details']
        })

    if articles:

        #print(f"Type of find_all() result: {type(articles)}")  # ResultSet object
        print(f"Number of articles found: {len(articles)}\n")

        results = []

        for article in articles:
            try:
                
                article_data = {
                    'listing_id': article.get('data-listing-id'),
                    'title': article.get('data-title'),
                    'installment': article.get('data-installment'),
                    'year': article.get('data-year', ''),
                    'variant': article.get('data-variant', ''),
                    'mileage': article.get('data-mileage', ''),
                    'transmission': article.get('data-transmission', ''),
                    'location': None,
                    'state': None,
                    'listing_image': article.get('data-image-src')

                }

                default_text = article.get('data-default-line-text', '')
                price, url = extract_price_and_url(default_text)
                article_data['price'] = price
                article_data['url'] = url

                if url:
                    location_data = extract_location_and_date(url)

                    if isinstance(location_data,dict):
                        for key, value in location_data.items():
                            if value is not None:
                                article_data[key] = value
                
                results.append(article_data)
                
            except Exception as e:
                print(f"Error processing article {article.get('data-listing-id', 'unknown')}: {str(e)}")
                continue  # Skip this article and continue with next one
        
        if results:
            df = pd.DataFrame(results)
            # Replace empty strings and None values with pd.NA
            df = df.replace(['', None], pd.NA)
            #print(df)
            return df
        else:
            print("No results were successfully processed")

def upload_to_datalake(csv_buffer): 

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
        file_name = f"vios_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"
        
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
        
        # Upload the CSV data from the buffer
        file_client.upload_data(csv_buffer.getvalue(), overwrite=True)
        
        print(f"File uploaded successfully to Azure Data Lake: {directory_path}/{file_name}")
        
    except Exception as e:
        print(f"Error uploading to Data Lake: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        print(traceback.format_exc())
    
    # Example call for debugging

