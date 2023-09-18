from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import requests
import json
from requests import Session
import re
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pathlib import Path


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
        from scrapingfunctions import data_to_csv
        data_to_csv(data_list, extention)

    @task()
    def cleaning_for_visual_task(data_list):
        data = pd.DataFrame(data_list)
        from cleaning_for_visual import cleaning_data
        cleaning_data(data)

    # Call get_all_urls to get the list of URLs
    url_list = get_all_urls_task(page=1)

    # Call scraper to scrape the data from the URLs
    data_list = scraper_task(url_list)

    data_to_csv_task(data_list,extention="urls/test.csv")


    # Clean the data for visualization
    cleaning_for_visual_task(data_list)
    
    

test_dag = immoweb_etl()