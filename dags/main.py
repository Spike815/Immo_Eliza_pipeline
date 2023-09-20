from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import requests
import os
from requests import Session
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import boto3
from io import StringIO

default_args = {
    'owner': 'bo',
    'retires': 5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id = 'dag_immo_pipeline_v1',
     default_args=default_args,
     start_date = datetime(2023,9,13,23,0,0),
     schedule='@daily')
def immoweb_etl():  
    from scrapingfunctions import get_house_info
    from scrapingfunctions import get_url,get_all_urls

    @task()
    def scraper_task(final_url_list): 
        from scrapingfunctions import scraper
        return scraper(final_url_list)
    
    @task()
    def get_all_urls_task(page):
        return get_all_urls(page)
    
    @task()
    def data_to_csv_task(data_list,extention):
        from data_to_csv import data_to_csv
        data_to_csv(data_list,extention)

    @task()
    def cleaning_for_visual_task(data_list):
        data = pd.DataFrame(data_list)
        from cleaning_for_visual import cleaning_data
        return cleaning_data(data)

    # Call get_all_urls to get the list of URLs
    url_list = get_all_urls_task(page=1)

    # Call scraper to scrape the data from the URLs
    data_list = scraper_task(url_list)

    #save data in s3 bucket
    data_to_csv_task(data_list,extention="raw_data.csv")

    # Clean the data for visualization and save in the csv bucket
    data_to_visual=cleaning_for_visual_task(data_list)
    data_to_csv_task(data_to_visual,extention="data_for_visual.csv")
    
    

test_dag = immoweb_etl()