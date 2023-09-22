from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv


default_args = {
    'owner': 'bo',
    'retires': 5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id = 'dag_immo_pipeline_v1',
     default_args=default_args,
     start_date = datetime(2023,9,13,23,0,0),
     schedule='@daily',
     catchup=False)
def immoweb_etl():  
    from data_to_csv import csv_to_data,data_to_csv
    from scrapingfunctions import get_url,get_all_urls
    from cleaning_for_visual import cleaning_data
    from cleaning_for_ml import cleaning_for_ml
    from machine_learning_model import build_ml

    @task()
    def scraper_task(): 
        from scrapingfunctions import scraper
        s3=csv_to_data("csv_files/urls_list.csv")
        df = pd.read_csv(s3["Body"])
        final_url_list=df["urls"].tolist()
        data_list=scraper(final_url_list)
        data = pd.DataFrame(data_list)
        data_to_csv(data,extention="raw_data.csv")
    
    @task()
    def get_all_urls_task(page):
        urls_list = get_all_urls(page)
        data = pd.DataFrame({"urls":urls_list})
        #send back the full list as csv file to s3 bucket
        data_to_csv(data,extention="urls_list.csv")

    @task()
    def cleaning_for_visual_task():
        raw_data = csv_to_data("csv_files/raw_data.csv")
        data = pd.read_csv(raw_data['Body'])
        cleaned_data=cleaning_data(data)
        data_to_csv(cleaned_data,"cleaned_data.csv")
    
    @task()
    def cleaning_for_ml_task():
        raw_data = csv_to_data("csv_files/cleaned_data.csv")
        data = pd.read_csv(raw_data['Body'])
        df = cleaning_for_ml(data)
        data_to_csv(df,extention="data_for_ml.csv")

    @task()
    def ml_model_build_task():
        raw_data = csv_to_data("csv_files/data_for_ml.csv")
        data = pd.read_csv(raw_data['Body'])
        build_ml(data)




    # Call scraper to scrape the data from the URLs, and save the result to raw_data.csv in s3
    t1 = get_all_urls_task(page = 300)

    t2 = scraper_task()

    # Clean the data for visualization and save in the csv bucket
    t3 = cleaning_for_visual_task()
    
    #cleaning data for machine learning
    t4 = cleaning_for_ml_task()

    #build up ml model and save to s3 bucket
    t5 = ml_model_build_task()


    t1 >> t2 >> t3 >> t4 >> t5

test_dag = immoweb_etl()