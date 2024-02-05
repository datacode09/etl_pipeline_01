# etl.py
from config import *
from data_processing_utils import *
import glob
import logging
import shutil
import time

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

def daily_load():
    start_time = time.time()
    try:
        check_mark_files()
        copy_pgp_files()
        generate_parquet_files()
        move_parquet_to_hdfs()
        delete_copied_pgp_files()
        log_processing_status()
        logging.info("Total ETL runtime: {:.2f} minutes".format((time.time() - start_time) / 60))
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        # Implement any additional error handling or notifications as required

if __name__ == "__main__":
    daily_load()
