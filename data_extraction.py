from database_utils import DatabaseConnector
from config import headers, base_url
import pandas as pd
import tabula

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        # Check if the specified table exists in the database
        table_names = self.db_connector.list_db_tables()
        if table_name not in table_names:
            raise ValueError(f"Table '{table_name}' not found in the database.")

        # Initialize the database engine
        engine = self.db_connector.init_db_engine()

        # Read the table into a Pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        return df
    
    def retrieve_pdf_data(self, link):
        self.link = link
        dfs = tabula.read_pdf(self.link)
        
        # Concatenate the list of DataFrames into a single DataFrame
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
        
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        try:
            response = requests.get(number_of_stores_endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get('number_stores')
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to retrieve the number of stores: {str(e)}")

    def retrieve_stores_data(self, number_of_stores, base_url, headers):
        store_data = []
        for store_number in range(1, number_of_stores + 1):
            store_url = f"{base_url}/{store_number}"
            try:
                response = requests.get(store_url, headers=headers)
                response.raise_for_status()
                data = response.json()
                store_data.append(data)
            except requests.exceptions.RequestException as e:
                print(f"Failed to retrieve store {store_number}: {str(e)}")

        df = pd.DataFrame(store_data)
        return df

    def extract_from_s3(s3_url):
        bucket_name = 'data-handling-public'
        object_key = 'products.csv'
        destination_file_location = '/home/ugochukwu/Documents/boto3/products.csv'  # Specify the full local path
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, object_key, destination_file_location)
        data_from_s3 = pd.read_csv(destination_file_location)
        return data_from_s3
        
    def extract_Json_data_from_web_link(json_link):
        Json_data = pd.read_json(json_link)
        return Json_data
        
if __name__ == "__main__":
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # Create an instance of the DatabaseConnector
    db = DatabaseConnector('db_creds.yaml')
    
    # Create an instance of the DataExtractor class
    data_extractor = DataExtractor(db)
    
    # Retrieve PDF data
    data_from_pdf = data_extractor.retrieve_pdf_data(url)
    
    # Specify the table name you want to read
    table_name = 'legacy_store_details'
    
    # Call the method to read the table and get a Pandas DataFrame
    table_dataframe = data_extractor.read_rds_table(table_name)
    
    # Now you can work with the 'data_from_pdf' and 'table_dataframe' as needed.
    data_from_pdf.head(3)  # Display the first few rows of the combined PDF data
