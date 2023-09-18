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
    from functions import get_house_info
    from functions import get_url
    @task()
    def scraper(final_url_list): 
        """This function takes one parameter final_url_list, returns a list of dictionary scraped from\
            immoweb page."""       
        data_list = []
        added = 0
        futures = []
        #build up pool of tasks to be executed
        with ThreadPoolExecutor(max_workers=15) as executor:
            with requests.Session() as session:
                for url in final_url_list:
                    futures.append(executor.submit(get_house_info,url,session))
                #try to excute the get_house_info function, append the result to final list
                #capture the errors and print out the url
                for item in futures:
                    try:
                        data_list.append(item.result())
                        # percent = 100*added/len(final_url_list)
                        # print(f"Data being processed : {round(percent,2)}%", end="\r")
                    except:
                        i = futures.index(item)
                        print(f"There is an error while scraping this website:{final_url_list[i]}")
                    finally:
                        added += 1
        return data_list
    
    @task()
    def get_all_urls(page = 200):
        """this function takes one parameter 'page'(int), returns a list of urls scraped from\
                immoweb search page. The list contains page*60 urls in total."""
        with ThreadPoolExecutor(max_workers=12) as executor:
            with requests.Session() as session:
                futures = [executor.submit(get_url, session, i) for i in range(1,page+1)]
                list_of_url_temp= [item.result() for item in futures]
                final_url_list = [element for innerList in list_of_url_temp for element in innerList]
        return final_url_list
    
    @task()
    def data_to_csv(data_list):
        df = pd.DataFrame(data_list)
        path = Path.cwd() / "urls/test.csv"
        if not path.exists():   
            path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path)


    # Call get_all_urls to get the list of URLs
    url_list = get_all_urls(page=2)

    # Call scraper to scrape the data from the URLs
    data_list = scraper(url_list)

    data_to_csv(data_list)
    
    

test_dag = immoweb_etl()