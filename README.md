# Real Estate Data Scraping and Processing with Airflow

In this project, I used Apache Airflow to automate the process of scraping real estate property listings from [immoweb](https://www.immoweb.be), cleaning the data for visualization, and preparing it for machine learning modeling. Here are the key highlights of this project:


### Data Scraping and Cleaning

- I utilized Apache Airflow as a scheduler to regularly scrape real estate property listings from immoweb.be. This automated process ensures that the data remains up-to-date.
- The collected data undergoes a rigorous cleaning process to remove inconsistencies and prepare it for further analysis.


### Data Storage with Versioning

- All cleaned and processed data files are stored in an Amazon S3 bucket.
- Automatic versioning is implemented to maintain data integrity and facilitate easy tracking of changes over time.

This project streamlines the data pipeline for real estate data, making it readily available for visualization and machine learning purposes. If you have any questions or need more information about this project, please feel free to reach out.


### Integration with XGBoost Model

- The cleaned and updated real estate data is fed into an XGBoost machine learning model.
- This integration allows the XGBoost model to continually learn from the latest data, improving its accuracy over time.

### Timeline

-The whole project takes 6 working days to finish.
