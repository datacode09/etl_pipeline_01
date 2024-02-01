import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pyodbc
import time
import os
import glob
import re
import json
import shutil
import logging
import traceback
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Constants
SRC_PATH = '/users/prieappcol/BreadReport'
FILEPATH_BREAD = '/users/prieappcol/Bread/'
FILEPATH_KATBAT = '/users/prieappcol/Katabat/'
FILEPATH_DATA = '/users/prieappcol/BreadReport/Data/'
PREFIX_DICT = {
    'AcctActReturnFile': 'cmc_aar_',
    'AgentReturnFile': 'cmc_atr_',
    'WorkflowReturnFile': 'cmc_wfr_',
    'ProcessedPmtsImportFile': 'pmt_',
    'PmtTransExportFile': 'pmt_',
    'AcctPlacementFile': 'apf_'
}

def initialize_spark_session():
    """Initialize Spark session"""
    spark = SparkSession.builder.appName("BreadIngestion") \
        .config("spark.sql.parquet.binaryAsString", "true") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.initialExecutors", "2") \
        .config("spark.executor.instances", "4") \
        .config("spark.dynamicAllocation.maxExecutors", "12") \
        .config("spark.driver.memory", "1g") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def load_bread_schemas():
    """Load Bread schemas from JSON"""
    with open(f'{SRC_PATH}/Data/bread_schemas.json', 'r') as fp:
        bread_schemas = json.load(fp)
    return bread_schemas

def check_files():
    """Check and validate files"""
    try:
        logging.info("Start checking mark file number")
        filelist_mrk_bread = glob.glob(f'{FILEPATH_BREAD}*{syncday}*.mrk')
        filelist_mrk_katbat = glob.glob(f'{FILEPATH_KATBAT}*{syncday}*.mrk')
        mark_file_number = len(filelist_mrk_bread) + len(filelist_mrk_katbat)

        bread_pgp_file_number = len(glob.glob(f'{FILEPATH_BREAD}*{syncday}*.pgp'))
        katabat_pgp_file_number = len(glob.glob(f'{FILEPATH_KATBAT}*{syncday}*.pgp'))

        expect_file_num = 7 if katabat_pgp_file_number >= 5 else 6

        logging.info(f"Bread Mark file number: {len(filelist_mrk_bread)}, Katabat Mark File Number: {len(filelist_mrk_katbat)}")
        logging.info(f"Bread PGP file number: {bread_pgp_file_number}, Katabat PGP file number: {katabat_pgp_file_number}")

        if mark_file_number < 6 and (bread_pgp_file_number + katabat_pgp_file_number) != expect_file_num:
            raise ValueError("Mark files or PGP files weren't loaded correctly")

        logging.info("Finished checking mark file number")
        return mark_file_number, bread_pgp_file_number, katabat_pgp_file_number
    except Exception as e:
        handle_error(e)

def copy_pgp_files():
    """Copy PGP files"""
    try:
        filelist_pgp_bread = glob.glob(f'{FILEPATH_BREAD}*{syncday}*.pgp')
        filelist_pgp_katbat = glob.glob(f'{FILEPATH_KATBAT}*{syncday}*.pgp')

        for file in filelist_pgp_bread + filelist_pgp_katbat:
            shutil.copy(file, FILEPATH_DATA)
            logging.info(f"Copying PGP file: {file}")
        logging.info("Finished copying PGP files")
    except Exception as e:
        handle_error(e)

def process_queue_export(file_path, bread_schemas):
    """Process QueueExport files"""
    try:
        df = pd.read_csv(file_path, delimiter='|', header=None, names=bread_schemas.get("queue_export"))
        df.insert(0, 'recordtype', 'QueueExport')
        df['snap_dt'] = datetime.strptime(file_path[-13:-5], '%Y%m%d')

        date_cols = [col for col in df.columns if ('date' in col) and ('update' not in col)]

        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')
            except:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d')

        return df
    except Exception as e:
        handle_error(e)

# Define other functions (remove_bad_data, initial_formatting, process_subrecords, move_files_to_hdfs, remove_local_files)

def daily_load():
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
        
        # Check and validate files
        validate_files()
        
        # Copy PGP files
        copy_pgp_files()
        
        # Generate Parquet files
        generate_parquet_files(bread_schemas)
        
        # Move Parquet files to HDFS
        move_parquet_files_to_hdfs()
        
        # Delete copied PGP files
        delete_copied_pgp_files()
        
        # Write successful log
        write_successful_log()
        
        logging.info(f"Processed all Katabat & Bread files. Runtime: {(time.time() - start_time) / 60:.2f} minutes")
        logging.info("===========================================")
    except Exception as e:
        handle_error(e)

def validate_files():
    """Check and validate files"""
    mark_file_number, bread_pgp_file_number, katabat_pgp_file_number = check_files()
    expect_file_num = 7 if katabat_pgp_file_number >= 5 else 6
    
    if mark_file_number < 6 and (bread_pgp_file_number + katabat_pgp_file_number) != expect_file_num:
        raise ValueError("Mark files or PGP files weren't loaded correctly")

def copy_pgp_files():
    """Copy PGP files"""
    filelist_pgp_bread = glob.glob(f'{filepath_bread}*{syncday}*.pgp')
    filelist_pgp_katbat = glob.glob(f'{filepath_katbat}*{syncday}*.pgp')
    
    for file in filelist_pgp_bread + filelist_pgp_katbat:
        shutil.copy(file, filepath_data)
        logging.info(f"Copying PGP file: {file}")

def generate_parquet_files(bread_schemas):
    """Generate Parquet files"""
    filelist_pgp_data = glob.glob(f'{filepath_data}*{syncday}*.pgp')
    
    for fn in filelist_pgp_data:
        if 'QueueExport' in fn:
            output = process_queue_export(fn, bread_schemas)
            out = pa.Table.from_pandas(output)
            pq.write_table(out, f'{src}/Data/CMC_QueueExport_QueueExport_{fn[-13:-5]}.parquet', use_deprecated_int96_timestamps=True)
        else:
            if 'AccountPlacementImportFile' in fn:
                remove_bad_data(fn)
            rec_df, name, snap_date = initial_formatting(fn)
            process_subrecords(rec_df, name, rec_df['recordtype'].unique().tolist(), snap_date)
            logging.info(f"{name} read completed")

def move_parquet_files_to_hdfs():
    """Move Parquet files to HDFS"""
    filelist_parquet_data = glob.glob(f'{filepath_data}*{syncday}.parquet')
    move_files_to_hdfs(filelist_parquet_data)

def delete_copied_pgp_files():
    """Delete copied PGP files"""
    filelist_pgp_data = glob.glob(f'{filepath_data}*{syncday}*.pgp')
    remove_local_files(filelist_pgp_data)

def write_successful_log():
    """Write successful log"""
    status = "Successfully completed"
    end = datetime.now()
    writeLog(taskID=task_id, taskname=task_name, shellscript=shell_name,
             timeperiod=None, taskStatus=status, taskMessage=None,
             startTime=start, endTime=end, recordCount=None)

def handle_error(e):
    """Handle errors and log them"""
    errLog = str(e)
    record_count = None
    errHandling(task_id, task_name, shell_name, errLog, record_count, recipients, start)

def write_successful_log():
    """Write successful log"""
    try:
        status = "Successfully completed"
        end = datetime.now()
        writeLog(taskID=task_id, taskname=task_name, shellscript=shell_name,
                 timeperiod=None, taskStatus=status, taskMessage=None,
                 startTime=start, endTime=end, recordCount=None)
        save_status_to_file("success")
        logging.info("Finished writing successful log")
    except Exception as e:
        handle_error(e)

def save_status_to_file(status):
    """Save the status to a .stat file"""
    try:
        with open(f'{SRC_PATH}/status.stat', 'w') as stat_file:
            stat_file.write(status)
        logging.info(f"Status saved to {SRC_PATH}/status.stat")
    except Exception as e:
        handle_error(e)

def handle_error(e):
    """Handle errors and log them"""
    try:
        errLog = str(e)
        record_count = None
        errHandling(task_id, task_name, shell_name, errLog, record_count, recipients, start)
        save_status_to_file("failure")
        logging.error(f"Error occurred: {errLog}")
    except Exception as e:
        print(f"Error handling error: {str(e)}")

if __name__ == "__main__":
    # Variable declaration & libraries
    # ...
    sys.path.insert(0, '/users/prieappcol/BreadReport/Conf')
    from secureconfig import *
    from connectdatabase import *
    # ...

    try:
        daily_load()
    except Exception as e:
        print(f"Unhandled error occurred: {str(e)}")
