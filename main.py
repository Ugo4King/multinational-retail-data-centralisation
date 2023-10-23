from sqlalchemy import create_engine, text
from database_utils import DataConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import boto3
import tabula
import yaml
import pandas as pd
import dateutil.parser as parser
import re
import requests


# Extract data from AWS RDS database clean it and load it into postgresql in my localhost

db_conection = DataConnector('db_creds.yaml') #Create an instance of the DataConnector class
reading_credentials = db_conection.read_db_cred() # Read the credentials from yaml file to access AWS RDS database.
create_db_engine = db_conection.init_db_engine() # Create SQL alchemy engine that connect to AWS RDS database.
list_the_table_in_db = db_conection.list_db_table()

# Extracting data from the AWS RDS database
db_connection = DataConnector('db_creds.yaml') #Calling the instance of the DataConnector class
table_name = 'legacy_users'
extract_table_data_from_AWS_RDS = DataExtractor(db_conection)#Calling the instance of the DataExtractor class 
user_data_table = extract_table_data_from_AWS_RDS.read_rds_table(table_name)
# Clean AWS RDS extracted data
clean_user_table = DataCleaning(user_data_table) #Calling the instance of the DataCleaning class
cleaned_user_data = clean_user_table.clean_user_data()
table_name_for_loading_user_data = 'dim_users'
uploading_user_data_to_psql = db_connection.upload_to_db(cleaned_user_data,table_name_for_loading_user_data) # Loading the user data table to psql

#Extracting card data from PDF
db_connection = DataConnector('db_creds.yaml') #Calling the instance of the DataConnector class
url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
extract_card_data_from_pdf = DataExtractor(db_conection)#Calling the instance of the DataExtractor class 
pdf_card_data_table = extract_card_data_from_pdf.retrieve_pdf_data(url)
# Clean card data in pdf extracted data
clean_card_data_table = DataCleaning(pdf_card_data_table) #Calling the instance of the DataCleaning class
cleaned_card_data = clean_card_data_table.clean_card_data()
table_name_for_loading_pdf_data = 'dim_card_details'
uploading_card_data_to_psql = db_connection.upload_to_db(cleaned_card_data,table_name_for_loading_pdf_data) # Loading the card data table to psql

#Extracting store data from API
db_connection = DataConnector('db_creds.yaml') #Calling the instance of the DataConnector class
base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
extract_store_data = DataExtractor(db_conection)#Calling the instance of the DataExtractor class 
number_of_stores = extract_store_data.list_number_of_stores(number_of_stores_endpoint, headers) # Get the number of stores
store_data_table = extract_store_data.retrieve_stores_data(number_of_stores, base_url, headers) # Retrieve store data and store it in a DataFrame
df = store_data_table
# Clean store data from API
clean_store_data_table = DataCleaning(df) #Calling the instance of the DataCleaning class
cleaned_store_data = clean_store_data_table.called_clean_store_data()
table_name_for_loading_store_data = 'dim_store_details'
uploading_store_data_to_psql = db_connection.upload_to_db(cleaned_store_data,table_name_for_loading_store_data) # Loading the store data table to psql

#Extracting product data from WAS S3 buket
db_connection = DataConnector('db_creds.yaml')  # Calling the instance of the DataConnector class 
extract_product_data = DataExtractor(db_connection)  # Calling the instance of the DataExtractor class 
product_data_table = extract_product_data.extract_from_s3()
# Clean product data from AWS S3 buket
clean_product_data_table = DataCleaning(product_data_table)#Calling the instance of the DataCleaning class
clean__weights_data_table = clean_product_data_table.convert_product_weights()
cleaned_product_data = clean_product_data_table.clean_products_data()
table_name_for_loading_product_data = 'dim_products'
uploading_product_data_to_psql = db_connection.upload_to_db(cleaned_product_data,table_name_for_loading_product_data) # Loading the product data table to psql

#Extracting order data from AWS RDS database
db_connection = DataConnector('db_creds.yaml') #Calling the instance of the DataConnector class
table_name = 'orders_table'
extract_user_table_data_from_AWS_RDS = DataExtractor(db_conection)#Calling the instance of the DataExtractor class 
user_data_table = extract_user_table_data_from_AWS_RDS.read_rds_table(table_name)
# Clean AWS RDS extracted data
clean_order_table = DataCleaning(user_data_table) #Calling the instance of the DataCleaning class
cleaned_order_data = clean_order_table.clean_orders_data()
uploading_order_data_to_psql = db_connection.upload_to_db(cleaned_order_data,table_name) # Loading the order data table to psql

#Extracting date data from JSON
db_connection = DataConnector('db_creds.yaml') #Calling the instance of the DataConnector class
url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
extract_date_data = DataExtractor(db_conection) #Calling the instance of the DataExtractor class 
date_data_table = extract_date_data.extract_Json_data_from_web_link(url)
# Clean date data from JSON
clean_date_data_table = DataCleaning(date_data_table) #Calling the instance of the DataCleaning class
cleaned_date_data = clean_date_data_table.clean_json_data()
table_name_for_loading_date_data = 'dim_date_times'
uploading_data_data_to_psql = db_connection.upload_to_db(cleaned_date_data,table_name_for_loading_date_data) # Loading the data data table to psql


