# etl.py
from config import *
from data_processing_utils import *
import glob
import logging
import shutil
import time
import os
from config import *
from pyspark.sql import SparkSession


def check_mark_files():
    logging.info("Checking mark files...")
    filelist_mrk_bread = glob.glob(filepath_bread + "*" + syncday + "*.mrk")
    filelist_mrk_katbat = glob.glob(filepath_katbat + "*" + syncday + "*.mrk")
    mark_file_number = len(filelist_mrk_bread) + len(filelist_mrk_katbat)
    logging.info(f"Bread Mark file number: {len(filelist_mrk_bread)}, Katabat Mark File Number: {len(filelist_mrk_katbat)}")
    
    # Example check, adjust according to your specific requirements
    expected_mark_files = 6  # Example expected number, adjust as necessary
    if mark_file_number < expected_mark_files:
        err_msg = f"Error: Expected at least {expected_mark_files} mark files, found {mark_file_number}."
        logging.error(err_msg)
        raise Exception(err_msg)
    else:
        logging.info("Mark file check passed.")

def copy_pgp_files():
    logging.info("Copying PGP files...")
    filelist_pgp_bread = glob.glob(filepath_bread + "*" + syncday + "*.pgp")
    filelist_pgp_katbat = glob.glob(filepath_katbat + "*" + syncday + "*.pgp")
    total_files_copied = 0
    
    for file in filelist_pgp_bread + filelist_pgp_katbat:
        try:
            shutil.copy(file, filepath_data)
            logging.info(f"Copied: {file}")
            total_files_copied += 1
        except Exception as e:
            logging.error(f"Failed to copy {file}: {e}")
    
    logging.info(f"Total PGP files copied: {total_files_copied}")


def generate_parquet_files():
    logging.info("Generating Parquet files...")
    filelist_pgp_data = glob.glob(filepath_data + "*" + syncday + "*.pgp")
    for fn in filelist_pgp_data:
        if 'QueueExport' in fn:
            output = process_queue_export(fn)
            pq.write_table(pa.Table.from_pandas(output), '{}/Data/CMC_QueueExport_QueueExport_{}.parquet'.format(src, fn[-13:-5]), use_deprecated_int96_timestamps=True)
        else:
            if 'AccountPlacementImportFile' in fn:
                remove_baddata(fn)
            rec_df, name, snap_date = initial_formatting(fn)
            process_subrecords(rec_df, name, rec_df['recordtype'].unique().tolist(), snap_date)

def move_parquet_to_hdfs():
    logging.info("Moving Parquet files to HDFS...")
    filelist_parquet_data = glob.glob(filepath_data + "*" + syncday + ".parquet")
    move_file_hdfs(filelist_parquet_data)

def delete_copied_pgp_files():
    logging.info("Deleting copied PGP files...")
    filelist_pgp_data = glob.glob(filepath_data + "*" + syncday + "*.pgp")
    remove_local_file(filelist_pgp_data)

def log_processing_status():
    logging.info("ETL Process completed successfully.")



def check_data_paths_availability():
    """
    Checks the availability of source directories, files, and destination paths with retries.
    Attempts up to 3 times with increasing wait time between retries.
    """
    max_retries = 3
    wait_time = 2  # Initial wait time in seconds, will double with each retry

    for attempt in range(1, max_retries + 1):
        try:
            # Check each source directory
            directories_to_check = [filepath_bread, filepath_katbat, filepath_data]
            for directory in directories_to_check:
                if not os.path.isdir(directory):
                    raise FileNotFoundError(f"Source directory not found: {directory}")

            # Check HDFS destination path accessibility (Example path)
            hdfs_destination_path = "/prod/01559/app/RIE0/data_tde/COLDataFiles/"
            hdfs_access_test = spark._jvm.org.apache.hadoop.fs.Path(hdfs_destination_path)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            if not fs.exists(hdfs_access_test):
                raise Exception(f"HDFS destination path not accessible: {hdfs_destination_path}")

            print("All source and destination paths are available and accessible.")
            return  # Exit the function upon successful check
        except Exception as e:
            if attempt < max_retries:
                print(f"Attempt {attempt} failed, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            else:
                print(f"All attempts failed. Final attempt error: {e}")
                raise


def daily_load():
    start_time = time.time()
    logging.info("Starting the ETL process.")
    
    try:
        logging.info("Checking data paths availability.")
        check_data_paths_availability()
        logging.info("Data paths availability check passed.")
        
        logging.info("Step 1: Checking mark files.")
        check_mark_files()
        logging.info("Completed: Mark files check successful.")

        logging.info("Step 2: Copying PGP files.")
        copy_pgp_files()
        logging.info("Completed: PGP files copied successfully.")

        logging.info("Step 3: Generating Parquet files.")
        generate_parquet_files()
        logging.info("Completed: Parquet files generated successfully.")

        logging.info("Step 4: Moving Parquet files to HDFS.")
        move_parquet_to_hdfs()
        logging.info("Completed: Parquet files moved to HDFS successfully.")

        logging.info("Step 5: Deleting copied PGP files.")
        delete_copied_pgp_files()
        logging.info("Completed: Copied PGP files deleted successfully.")

        logging.info("ETL Process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed at {time.strftime('%Y-%m-%d %H:%M:%S')}: {str(e)}")
        # It's helpful to re-raise the exception if the process is being monitored or further error handling is required
        raise e
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Total ETL runtime: {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    daily_load()
