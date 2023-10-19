from sqlalchemy import create_engine, text
import yaml
import pandas as pd

class DataConnector:
    def __init__(self, config_file):
        self.config_file = config_file

    def read_db_cred(self):
        with open(self.config_file, 'r') as file:
            creds = yaml.safe_load(file)
            return creds

    def init_db_engine(self):
        creds = self.read_db_cred()
        host = creds['RDS_HOST']
        password = creds['RDS_PASSWORD']
        user = creds['RDS_USER']
        db_name = creds['RDS_DATABASE']
        port = creds['RDS_PORT']
        conn = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(conn)
        return engine

    def list_db_table(self):
        engine = self.init_db_engine()
        with engine.connect() as connection:
            query = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"""
            table_names = connection.execute(text(query))
            return [table[0] for table in table_names]

    def upload_to_db(self, df, table_name):
        cred = self.read_db_cred()
        HOST = cred['HOST']
        PASSWORD = cred['PASSWORD']
        USER = cred['USER']
        DATABASE = cred['DATABASE']
        PORT = cred['PORT']
        
        new_engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, new_engine, if_exists='replace', index=False)
        return new_engine

 if __name__ == "__main__":
    db_conection = DataConnector('db_creds.yaml') #Create an instance of the DataConnector class
    reading_credentials = db_conection.read_db_cred() # Read the credentials from yaml file to access AWS RDS database.
    create_db_engine = db_conection.init_db_engine() # Create SQL alchemy engine that connect to AWS RDS database.
    list_the_table_in_db = db_conection.list_db_table()
