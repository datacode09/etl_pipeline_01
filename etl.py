import logging
import warnings
import time
import sys
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from utilities import *

# Set up logging
logging.basicConfig(filename='/users/prieappcol/BreadReport/Log/User_Log/BreadIngestion_Daily.log', level=logging.INFO)

# Define constants
today = datetime.today().strftime("%Y-%m-%d")
syncday = os.getenv("syncday")

def extract_data():
    try:
        # Extract data from sources (e.g., copy PGP files)
        # ...
        logging.info("Data extraction completed successfully")
        return True
    except Exception as e:
        logging.error(f"Error during data extraction: {str(e)}")
        write_status_to_file(today, "Extraction", "Failed")
        return False

def transform_data():
    try:
        # Transform the extracted data (e.g., generate Parquet files)
        # ...
        logging.info("Data transformation completed successfully")
        return True
    except Exception as e:
        logging.error(f"Error during data transformation: {str(e)}")
        write_status_to_file(today, "Transformation", "Failed")
        return False

def load_data():
    try:
        # Load transformed data into HDFS (e.g., move Parquet files to HDFS)
        # ...
        logging.info("Data loading completed successfully")
        return True
    except Exception as e:
        logging.error(f"Error during data loading: {str(e)}")
        write_status_to_file(today, "Loading", "Failed")
        return False

# Define the phases as functions in a list
phases = [extract_data, transform_data, load_data]

def run_etl():
    try:
        logging.info("===========================================")
        logging.info("Sync Start")
        logging.info("Today: " + today)
        logging.info("Sync Date: " + syncday[:4] + "-" + syncday[4:6] + "-" + syncday[6:])
        recipients = ['teresa.nguyen@rbc.com', 'cbodatac@rbc.com']
        start_time = time.time()
        
        # Initialize Spark session
        spark = initialize_spark_session()
        
        # Load Bread schemas
        bread_schemas = load_bread_schemas()
        
        # Execute the phases
        for phase in phases:
            if not phase():
                return
        
        # Write successful log
        status = "Successfully completed"
        end = datetime.now()
        write_success_log(start_time, task_id, task_name, shell_name)
        
        logging.info(f"Processed all Katabat & Bread files. Runtime: {(time.time() - start_time) / 60:.2f} minutes")
        logging.info("===========================================")
    except Exception as e:
        err_log = str(e)
        record_count = None
        err_handling(task_id, task_name, shell_name, err_log, record_count, recipients, start_time)

if __name__ == "__main__":
    run_etl()
