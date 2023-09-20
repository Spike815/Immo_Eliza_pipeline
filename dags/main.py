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
        from data_to_csv import data_to_csv
        data_list=scraper(final_url_list)
        data_to_csv(data_list,extention="raw_data.csv")
    
    @task()
    def get_all_urls_task(page):
        return get_all_urls(page)
    
    @task()
    def cleaning_for_visual_task():
        from cleaning_for_visual import cleaning_data
        from data_to_csv import csv_to_data,data_to_csv
        raw_data = csv_to_data("csv_files/raw_data.csv")
        data = pd.read_csv(raw_data['Body'])
        cleaned_data=cleaning_data(data)
        data_to_csv(cleaned_data,"cleaned_data.csv")

    # Call scraper to scrape the data from the URLs, and save the result to raw_data.csv in s3
    #t1 is to call function get_all_urls_task()
    t2 = scraper_task(get_all_urls_task(page=3))

    # Clean the data for visualization and save in the csv bucket
    t3 = cleaning_for_visual_task()

    t2 >>t3 

test_dag = immoweb_etl()