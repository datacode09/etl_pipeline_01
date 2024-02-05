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

def save_checkpoint(stage):
    with open('etl_checkpoint.txt', 'w') as f:
        f.write(stage)

def load_checkpoint():
    try:
        with open('etl_checkpoint.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None  # No checkpoint found
        
def daily_load():
    start_time = time.time()
    logging.info("Starting the ETL process.")
    
    # Load the last checkpoint, if it exists
    last_checkpoint = load_checkpoint()
    steps = [
        ("check_mark_files", check_mark_files),
        ("copy_pgp_files", copy_pgp_files),
        ("generate_parquet_files", generate_parquet_files),
        ("move_parquet_to_hdfs", move_parquet_to_hdfs),
        ("delete_copied_pgp_files", delete_copied_pgp_files)
    ]
    
    for step_name, step_function in steps:
        if last_checkpoint is None or last_checkpoint == step_name or steps.index((step_name, step_function)) > steps.index((last_checkpoint, globals()[last_checkpoint])):
            try:
                logging.info(f"Starting {step_name}.")
                step_function()
                save_checkpoint(step_name)  # Save after successful completion of each step
                logging.info(f"Completed {step_name} successfully.")
            except Exception as e:
                logging.error(f"ETL process failed during {step_name}: {e}")
                raise  # Optionally, re-raise the exception for external handling
            finally:
                elapsed_time = time.time() - start_time
                logging.info(f"ETL process stopped at {step_name}. Total runtime so far: {elapsed_time:.2f} seconds.")
                return  # Stop the process if an exception occurs

    # If all steps complete successfully, clear the checkpoint
    save_checkpoint("")  # Or delete the checkpoint file if preferred
    logging.info("ETL Process completed successfully.")


if __name__ == "__main__":
    daily_load()
