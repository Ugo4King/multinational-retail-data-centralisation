import yaml
from sqlalchemy import create_engine, text
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import dateutil.parser as parser
import re

class DataCleaning:
    def __init__(self, df):
        self.df = df

    def clean_user_data(self):
        # Clean and standardize user data
        self.df['country_code'] = self.df['country_code'].str.replace('GGB', 'GB')  # Replace GGB with GB country code
        self.df = self.df[~(self.df['country_code'].str.len() > 2)]  # Drop rows with a 'country_code' value length greater than 2 (e.g., 'US', 'GB', 'DE')
        self.df['phone_number'] = self.df['phone_number'].str.replace(r'[^x0-9]', '', regex=True)  # Remove non-numeric characters
        self.df['phone_number'] = self.df['phone_number'].str.split('x').str.get(0)  # Extract the phone number
        self.df['phone_number'] = self.df.apply(lambda x: x['phone_number'][-10] if len(x['phone_number']) > 10 else x['phone_number'], axis=1)  # Ensure the phone number is 10 digits
        self.df['phone_number'] = self.df.apply(lambda row: '+1' + row['phone_number'] if row['country_code'] == 'US' else
                                                '+44' + row['phone_number'] if row['country_code'] == 'GB' else '+49' + row['phone_number'] if row['country_code'] == 'DE' else
                                                row['phone_number'], axis=1)  # Add country codes
        self.df['clean_address'] = self.df.apply(lambda x: x['address_1'] + ' ' + x['address_2'] + ' ' + x['address_3']
                                                  if pd.notna(x['address_4']) else x['address_1'] + ' ' + x['address_2'], axis=1)  # Combine address fields
        self.df['address'] = self.df['address'].str.replace('\n', ',')  # Replace newline characters with commas
        self.df[['address_1', 'address_2', 'address_3', 'address_4']] = self.df['address'].str.split(',', expand=True)  # Split the 'address' column
        self.df['address_4'] = self.df.apply(lambda x: x['address_3'] if pd.isnull(x['address_4']) else x['address_4'], axis=1)  # Set 'address_4' based on 'address_3'
        self.df.drop(['address_1', 'address_2', 'address_3'], axis=1, inplace=True)  # Drop unnecessary columns
        self.df.rename(columns={'address_4': 'post_code'}, inplace=True)  # Rename 'address_4' to 'post_code'
        column1_to_move = self.df.pop('clean_address')  # Move 'clean_address' column
        self.df.insert(7, 'clean_address', column1_to_move)
        column2_to_move = self.df.pop('post_code')  # Move 'post_code' column
        self.df.insert(8, 'post_code', column2_to_move)
        self.df['date_of_birth'] = self.df['date_of_birth'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))  # Parse and format 'date_of_birth'
        self.df['join_date'] = self.df['join_date'].apply(date_format)  # Assuming 'date_format' is a valid function
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], format='%Y-%m-%d')  # Convert 'date_of_birth' to datetime
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], format='%Y-%m-%d')  # Convert 'join_date' to datetime
        return self.df  # Return the cleaned DataFrame

    def clean_card_data(self):
        # Clean and standardize card data
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], format='%Y-%m-%d')  # Convert 'date_payment_confirmed' to datetime
        self.df['card_provider'] = self.df['card_provider'].str.replace('/', 'and')  # Replace '/' with 'and' in 'card_provider'
        self.df['card_provider'] = self.df['card_provider'].apply(lambda x: 'VISA' if x.startswith('VISA') else ('JCB' if x.startswith('JCB') else x))  # Standardize card provider names
        self.df['card_provider'] = self.df['card_provider'].astype('category')  # Convert 'card_provider' to a category
        return self.df  # Return the cleaned DataFrame

    def clean_date_format(self):
        # Clean date formats and select specific countries
        self.df['continent'] = self.df['continent'].str.replace('eeEurope', 'Europe')  # Replace 'eeEurope' with 'Europe'
        self.df['continent'] = self.df['continent'].str.replace('eeAmerica', 'America')  # Replace 'eeAmerica' with 'America'
        selected_countries = ['GB', 'DE', 'US']  # List of selected countries
        self.df = self.df[self.df['country_code'].isin(selected_countries)]  # Filter data for specific countries
        self.df = self.df.drop('lat', axis=1)  # Drop the 'lat' column
        self.df = self.df.dropna()  # Drop rows with missing values
        self.df['opening_date'] = self.df['opening_date'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))  # Parse and format 'opening_date'
        self.df['staff_numbers'] = self.df['staff_numbers'].str.replace(r'\D', '', regex=True)  # Remove non-numeric characters from 'staff_numbers'
        self.df['staff_numbers'] = self.df['staff_numbers'].astype(int)  # Convert 'staff_numbers' to an integer
        self.df['longitude'] = self.df['longitude'].astype(float)  # Convert 'longitude' to a float
        self.df['latitude'] = self.df['latitude'].astype(float)  # Convert 'latitude' to a float
        self.df['store_type'] = self.df['store_type'].astype('category')  # Convert 'store_type' to a category
        self.df['country_code'] = self.df['country_code'].astype('category')  # Convert 'country_code' to a category
        self.df['continent'] = self.df['continent'].astype('category')  # Convert 'continent' to a category
        self.df['address'] = self.df['address'].str.replace('\n', ',')  # Replace newline characters with commas
        return self.df  # Return the cleaned DataFrame

    def convert_product_weights(self):
        # Convert product weights to a consistent unit (e.g., kg)
        unit_conversions = {
                'g': 0.001,
                'kg': 1,
                'ml': 0.001,
            }
        def convert_weight(weight_str):
            match = re.match(r'(\d+(\.\d+)?)\s*(\w+)', str(weight_str)  # Match weight values
            if match:
                value, unit = float(match.group(1)), match.group(3).lower()
                return value * unit_conversions.get(unit, 1)  # Convert weight to kg based on unit conversions
        self.df['weight'] = self.df['weight'].apply(convert_weight)  # Apply the weight conversion function
        self.df.dropna(subset=['weight'], inplace=True)  # Remove rows with missing weight information
        return self.df  # Return the cleaned DataFrame

    def clean_products_data(self):
        # Clean and standardize product data
        self.df.drop('Unnamed: 0', axis=1, inplace=True)  # Drop the 'Unnamed: 0' column
        self.df.reset_index(drop=True, inplace=True)  # Reset the DataFrame index
        self.df.index.name = 'index'  # Set the index name to 'index'
        self.df['product_price'] = self.df['product_price'].str.strip('£')  # Remove '£' symbol from 'product_price'
        self.df['product_price'] = self.df['product_price'].astype(float)  # Convert 'product_price' to a float
        self.df['category'] = self.df['category'].astype('category')  # Convert 'category' to a category
        self.df['EAN'] = self.df['EAN'].astype(int)  # Convert 'EAN' to an integer
        self.df['date_added'] = self.df['date_added'].apply(lambda x: pd.to_datetime(parser.parse(x, dayfirst=True)).strftime('%Y-%m-%d'))  # Parse and format 'date_added'
        self.df['date_added'] = pd.to_datetime(self.df['date_added'], format='%Y-%m-%d')  # Convert 'date_added' to datetime
        self.df['removed'] = self.df['removed'].astype('category')  # Convert 'removed' to a category
        return self.df  # Return the cleaned DataFrame

    def clean_orders_data(self):
        # Clean and standardize orders data
        self.df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)  # Drop unnecessary columns
        self.df.drop('level_0', axis=1, inplace=True)  # Drop the 'level_0' column
        self.df.drop('index', axis=1, inplace=True)  # Drop the 'index' column
        self.df.index.name = 'index'  # Set the index name to 'index'
        return self.df  # Return the cleaned DataFrame

    def clean_json_data(self):
        # Clean and standardize JSON data
        self.df.reset_index(drop=True, inplace=True)  # Reset the DataFrame index
        self.df.index.name = 'index'  # Set the index name to 'index'
        self.df = self.df[self.df['day'].str.isnumeric()]  # Filter data for rows with numeric 'day' values
        self.df['datetime'] = pd.to_datetime(self.df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' + self.df['timestamp'])  # Create a 'datetime' column
        self.df['time_period'] = self.df['time_period'].astype('category')  # Convert 'time_period' to a category
        return self.df  # Return the cleaned DataFrame
