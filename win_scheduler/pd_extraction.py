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

def main():

    base_url = ['https://www.carlist.my/cars-for-sale/perodua/myvi/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc', \
                'https://www.carlist.my/cars-for-sale/honda/city/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc', \
                'https://www.carlist.my/cars-for-sale/toyota/vios/malaysia?page_number=X&page_size=25&sort=modification_date_search.desc' ]

    for x in base_url:
        all_json_data = []
        model_name = get_model_name(x)
        page_list = page_number(1,15,x)
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
        
        if all_json_data:
            # Convert to DataFrame
            df = pd.DataFrame(all_json_data)
            
            # Create filename with current date
            current_date = datetime.datetime.now().strftime("%Y%m%d")
            filename = f'{model_name}_{current_date}.parquet'
            
            # Save as parquet
            df.to_parquet(filename, index=False)
            print(f"Saved {len(all_json_data)} entries to {filename}")

if __name__ == "__main__":
    main()