from sqlalchemy import create_engine, text
from database_utils import DataConnector
from data_extraction import DataExtractor
import yaml
import numpy as np
import boto3
import pandas as pd
import dateutil.parser as parser
import re
import requests

class DataCleaning:
    def __init__(self, df):
        self.df = df

    def clean_user_data(self):
        self.df['country_code'] = self.df['country_code'].str.replace('GGB', 'GB') # replace GGB with GB country code
        self.df = self.df[~(self.df['country_code'].str.len() > 2)] # drop any row with a value length greater than 2 (US, GB, DE)
        self.df['phone_number'] = self.df['phone_number'].str.replace(r'[^x0-9]', '', regex=True)
        self.df['phone_number'] = self.df['phone_number'].str.split('x').str.get(0)
        self.df['phone_number'] = self.df['phone_number'].str[-10:]
        self.df['phone_number'] = self.df.apply(lambda row: '+1' + row['phone_number'] if row['country_code'] == 'US' else
        '+44' + row['phone_number'] if row['country_code'] == 'GB' else '+49' + row['phone_number'] if row['country_code'] == 'DE' else
        row['phone_number'], axis=1)
        self.df['address'] = self.df['address'].str.replace('\n', ',')
        self.df[['address_1', 'address_2', 'address_3', 'address_4']] = self.df['address'].str.split(',', expand=True)
        self.df['clean_address'] = self.df.apply(lambda x: x['address_1'] + ' ' + x['address_2'] + ' ' + x['address_3']
              if pd.notna(x['address_4']) else x['address_1'] + ' ' + x['address_2'], axis=1)
        self.df['address_4'] = self.df.apply(lambda x: x['address_3'] if pd.isnull(x['address_4']) else x['address_4'], axis=1)
        self.df.drop(['address_1', 'address_2', 'address_3'], axis=1, inplace=True)
        self.df.rename(columns={'address_4': 'post_code'}, inplace=True)
        column1_to_move = self.df.pop('clean_address')
        self.df.insert(7, 'clean_address', column1_to_move)
        column2_to_move = self.df.pop('post_code')
        self.df.insert(8, 'post_code', column2_to_move)
        self.df['date_of_birth'] = self.df['date_of_birth'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))
        self.df['join_date'] = self.df['join_date'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], format='%Y-%m-%d')
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], format='%Y-%m-%d')
        return self.df
        
    def clean_card_data(self):
        self.df.replace('NULL', np.nan, inplace=True)
        self.df = self.df.dropna()
        self.df = self.df[self.df['date_payment_confirmed'].str.match(r'\d{4}-\d{2}-\d{2}')]
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], format='%Y-%m-%d')
        self.df['card_provider'] = self.df['card_provider'].str.replace('/', 'and')
        self.df['card_provider'] = self.df['card_provider'].apply(lambda x: 'VISA' if x.startswith('VISA') else ('JCB' if x.startswith('JCB') else x))
        self.df['card_provider'] = self.df['card_provider'].astype('category')        
        return self.df
        
    def called_clean_store_data(self):
        self.df['continent'] = self.df['continent'].str.replace('eeEurope', 'Europe')
        self.df['continent'] = self.df['continent'].str.replace('eeAmerica', 'America')
        selected_countries = ['GB', 'DE', 'US']
        self.df = self.df[self.df['country_code'].isin(selected_countries)]
        self.df['opening_date'] = self.df['opening_date'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))
        self.df['staff_numbers'] = self.df['staff_numbers'].str.replace(r'\D', '', regex=True)
        self.df['longitude'] = self.df['longitude'].replace('N/A', np.nan)
        self.df['latitude'] = self.df['latitude'].replace('N/A', np.nan)
        self.df['staff_numbers'] = self.df['staff_numbers'].astype(int)
        self.df['store_type'] = self.df['store_type'].astype('category')
        self.df['country_code'] = self.df['country_code'].astype('category')
        self.df['continent'] = self.df['continent'].astype('category')
        self.df['address'] = self.df['address'].str.replace('\n', ',')
        return self.df
        
    def convert_product_weights(self):
        unit_conversions = {
            'g': 0.001,
            'kg': 1,
            'ml': 0.001,
        }

        def convert_weight(weight_str):
            match = re.match(r'(\d+(\.\d+)?)\s*(\w+)', str(weight_str))
            if match:
                value, unit = float(match.group(1)), match.group(3).lower()
                return value * unit_conversions.get(unit, 1)

        self.df['weight'] = self.df['weight'].apply(convert_weight)
        self.df.dropna(subset=['weight'], inplace=True)
        return self.df

    def clean_products_data(self):
        # Create a new DataFrame for the final result
        self.df.drop('Unnamed: 0', axis=1, inplace=True)
        self.df.reset_index(drop=True, inplace=True)
        self.df.index.name = 'index'
        self.df['product_price'] = self.df['product_price'].str.replace('£', '').str.strip()
        self.df['product_price'] = self.df['product_price'].str.extract(r'(\d+\.\d+)', expand=False)
        self.df['product_price'] = self.df['product_price'].astype(float)
        self.df['category'] = self.df['category'].astype('category')
        self.df = self.df[self.df['EAN'].str.isnumeric()]
        self.df['EAN'] = self.df['EAN'].astype(int)
        self.df['date_added'] = self.df['date_added'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))
        self.df['date_added'] = pd.to_datetime(self.df['date_added'], format='%Y-%m-%d')
        self.df['removed'] = self.df['removed'].astype('category')
        return self.df
        
    def  clean_orders_data(self):
        self.df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        self.df.drop('level_0', axis=1, inplace=True)
        self.df.drop('index', axis=1, inplace=True)
        self.df.index.name ='index'
        return self.df
        
    def clean_json_data(self):
        self.df.reset_index(drop=True, inplace=True)
        self.df.index.name ='index'
        self.df = self.df[self.df['day'].str.isnumeric()]
        self.df['datetime'] = pd.to_datetime(self.df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' + self.df['timestamp'])
        self.df['time_period'] = self.df['time_period'].astype('category')
        
        return self.df


# if __name__ == "__main__":
#     conn = DatabaseConnector('db_creds.yaml')
    
#     # Specify the table name
#     table_name = 'legacy_users'
    
#     # Create a DataExtractor instance and read data from the table
#     data_extractor = DataExtractor(conn)
#     dataframe = data_extractor.read_rds_table(table_name)
    
#     # Create an instance of DataCleaning and perform data cleaning
#     imported_table = DataCleaning(dataframe)
#     imported_table.clean_user_data()
#     imported_table.clean_phone_numbers()
#     imported_table.clean_address()
#     imported_table.clean_dates()
#     # imported_table.clean_pdf_data()