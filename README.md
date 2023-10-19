# Data Extraction and Cleaning Project

This repository contains code for extracting data from various sources, including databases, PDF files, and web links, and cleaning the data for further analysis and storage in a PostgreSQL database. The project is organized into several Python scripts, each serving a specific purpose. Below, we provide an overview of the project structure, the main components, and how to run the code.

## Table of Contents

1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Components](#components)
    1. [database_utils.py](#database-utils)
    2. [data_extraction.py](#data-extraction)
    3. [data_cleaning.py](#data-cleaning)
    4. [main.py](#main)
4. [Getting Started](#getting-started)
    1. [Prerequisites](#prerequisites)
    2. [Installation](#installation)
5. [Running the Project](#running-the-project)
6. [Contributing](#contributing)
7. [License](#license)

## Introduction <a name="introduction"></a>

This project focuses on data extraction and cleaning. It includes scripts for extracting data from different sources and preparing it for analysis or storage. The main data sources include AWS RDS databases, PDF files, web links, and JSON files. Each script in this project serves a specific purpose in the data extraction and cleaning process.

## Project Structure <a name="project-structure"></a>

The project is structured as follows:

- `database_utils.py`: Contains a class for connecting to an AWS RDS database, listing available tables, and uploading Pandas DataFrames to the database.

- `data_extraction.py`: Contains a class for extracting data from various sources, such as an AWS RDS database, PDF files, and web links. It also includes methods for data extraction from an S3 bucket and JSON files.

- `data_cleaning.py`: Contains a class for cleaning data in a Pandas DataFrame. It includes methods for cleaning user data, card data, store data, product data, order data, and date data. The cleaning process includes data type conversions, regular expression operations, and data manipulation.

- `main.py`: Demonstrates how to use the above classes to extract data from various sources, clean it, and load it into a PostgreSQL database.

## Components <a name="components"></a>

### database_utils.py <a name="database-utils"></a>

This script contains a class for connecting to an AWS RDS database, listing available tables, and uploading Pandas DataFrames to the database. Here are the key components of this script:

- `DataConnector`: This class initializes a connection to the database using the provided configuration file. It provides methods for reading database credentials, initializing the database engine, listing tables, and uploading dataframes to the database.

### data_extraction.py <a name="data-extraction"></a>

This script contains a class for extracting data from various sources, such as an AWS RDS database, PDF files, web links, S3 buckets, and JSON files. Here are the key components of this script:

- `DataExtractor`: This class is used to extract data from different sources. It includes methods for reading tables from an AWS RDS database, retrieving data from a PDF file, listing the number of stores from a web link, and extracting data from an S3 bucket and JSON files.

### data_cleaning.py <a name="data-cleaning"></a>

This script contains a class for cleaning data in a Pandas DataFrame. It includes methods for cleaning user data, card data, store data, product data, order data, and date data. The cleaning process involves operations such as data type conversions, regular expressions, and data manipulation.

### main.py <a name="main"></a>

This script demonstrates how to use the classes defined in the other scripts to extract data from various sources, clean it, and load it into a PostgreSQL database. It provides examples of how to use the classes and methods to perform these tasks.

## Getting Started <a name="getting-started"></a>

To run this project, you need Python installed on your system. You should also have access to an AWS RDS database and a PostgreSQL database where you want to load the cleaned data.

### Prerequisites <a name="prerequisites"></a>

- Python 3.x
- PostgreSQL database (for data storage)
- AWS RDS database (for data extraction)
- Pandas, SQLAlchemy, and other Python libraries (installed as per the requirements)

### Installation <a name="installation"></a>

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/your-username/data-extraction-cleaning.git
   ```

2. Install the required Python libraries:

   ```bash
   pip install pandas sqlalchemy tabula-py psycopg2-binary
   ```

3. Configure the database credentials:

   Create a YAML file named `db_creds.yaml` in the project directory and add your AWS RDS and PostgreSQL database credentials:

   ```yaml
   AWS_RDS:
     RDS_HOST: your-aws-rds-host
     RDS_DATABASE: your-aws-rds-database
     RDS_USER: your-aws-rds-username
     RDS_PASSWORD: your-aws-rds-password
     RDS_PORT: your-aws-rds-port

   PostgreSQL:
     HOST: your-postgresql-host
     DATABASE: your-postgresql-database
     USER: your-postgresql-username
     PASSWORD: your-postgresql-password
     PORT: your-postgresql-port
   ```

4. Ensure that you have the necessary data sources available, such

 as PDF files, web links, S3 buckets, and JSON files.

## Running the Project <a name="running-the-project"></a>

1. Run the main script to extract and clean data from various sources and load it into your PostgreSQL database:

   ```bash
   python main.py
   ```

   This script demonstrates how to use the defined classes and methods to extract, clean, and load data. You can modify this script to specify which data sources you want to work with and where to store the cleaned data.

## Contributing <a name="contributing"></a>

Contributions to this project are welcome. If you find issues or have suggestions for improvements, please open a GitHub issue and provide details. You can also submit pull requests to contribute code enhancements.

## License <a name="license"></a>

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Thank you for exploring this data extraction and cleaning project. If you have any questions or need further assistance, please feel free to contact the project maintainers.