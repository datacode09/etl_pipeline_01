import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import glob
import re
import shutil
from datetime import datetime
from pyspark.sql import SparkSession

# Define constants
src = '/users/prieappcol/BreadReport'
prefix_dict = {'AcctActReturnFile': 'cmc_aar_', 'AgentReturnFile': 'cmc_atr_', 'WorkflowReturnFile': 'cmc_wfr_',
               'ProcessedPmtsImportFile': 'pmt_', 'PmtTransExportFile': 'pmt_', 'AcctPlacementFile': 'apf_'}

def load_bread_schemas():
    """Load Bread schemas from JSON file"""
    with open(f'{src}/Data/bread_schemas.json', 'r') as fp:
        bread_schemas = json.load(fp)
    return bread_schemas

def initialize_spark_session():
    """Initialize SparkSession with configurations"""
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

def remove_bad_data(file_path):
    """Remove bad data from a CSV file"""
    problem_strings = [("\"syedasimhussain|@gmail.com\"", "syedasimhussain@gmail.com"),
                       ("Milliken Bridlewood Vet Clinic | 宠物医院, 2770 Kennedy Rd, Scarborough, ON M1T 3J2",
                        "Milliken Bridlewood Vet Clinic , 宠物医院, 2770 Kennedy Rd, Scarborough, ON M1T 3J2")]
    
    try:
        with open(file_path, "r") as csvReader:
            csv_content = ''.join([i for i in csvReader])
            for problem_str, correct_str in problem_strings:
                csv_content = csv_content.replace(problem_str, correct_str)
        with open(file_path, "w") as csvWriter:
            csvWriter.writelines(csv_content)
    except Exception as e:
        logging.error(e)

def initial_formatting(file_path):
    """Perform initial formatting of input Bread/Katabat files"""
    df = pd.read_csv(file_path, header=None, names=['data'], delimiter='*****')
    file_name = df['data'][0].split('|')[0]
    snap_dt = df['data'][0].split('|')[2]
    
    if file_name == 'HDR':
        file_name = df['data'][0].split('|')[1]
        snap_dt = df['data'][0].split('|')[2][:8]
    
    if file_name in ['WorkflowReturnFile', 'ProcessedPmtsImportFile', 'PmtTransExportFile', 'AcctPlacementFile']:
        df['recordtype'] = file_name
        df['custom'] = df['data']
        df = df[~df['data'].str.contains(file_name)][['recordtype', 'custom']].reset_index(drop=True)
    else:
        df[['recordtype', 'custom']] = df['data'].str.split('|', 1, expand=True)
        df = df[df['recordtype'] != file_name][['recordtype', 'custom']].reset_index(drop=True)
    
    return df, file_name, snap_dt

def process_subrecords(df, file_name, record_list, dt):
    """Process subrecords within a file"""
    prefix = prefix_dict.get(file_name)
    
    for rec in record_list:
        records = df[df['recordtype'] == rec]
        schema = bread_schemas.get('{}{}'.format(prefix, rec.lower()))
        output_path = f'{src}/Data/CMC_{file_name}_{rec}_{dt}.parquet'
        
        if file_name == 'AcctPlacementFile':
            try:
                records[schema] = records['custom'].str.split('|', expand=True)
            except Exception as e:
                print(e)
                split_rec = records['custom'].str.split('|', expand=True)
                schema_temp = schema.copy()
                schema_temp.append('temp')
                split_rec.columns = schema_temp
                split_rec = split_rec.drop(columns=['temp'])
                records = pd.concat([records, split_rec], axis=1)
        else:
            try:
                records[schema] = records['custom'].str.split('|', expand=True)
            except:
                split_rec = records['custom'].str.split('|', expand=True)
                split_rec.columns = schema
                records = pd.concat([records, split_rec], axis=1)
        
        records = records.drop(columns=['custom']).reset_index(drop=True)
        date_cols = [col for col in records.columns if ('date' in col) and ('update' not in col)]
        
        for col in date_cols:
            records.loc[records[col] == '', col] = ''
            try:
                records[col] = pd.to_datetime(records[col], format='%Y%m%d%H%M%S')
            except:
                records[col] = pd.to_datetime(records[col], format='%Y%m%d', errors='coerce')
        
        if file_name == 'AcctPlacementFile':
            double_cols = ['creditline', 'balance', 'currentdue', 'pastdue', 'chargeoffamount',
                           'originalchargeoffamount', 'fixedpaymentamount', 'availablecredit',
                           'feesoutstanding', 'principaloutstanding', 'interestoutstanding',
                           'bucketamount1', 'bucketamount2', 'bucketamount3', 'bucketamount4',
                           'bucketamount5', 'bucketamount6', 'bucketamount7', 'clientdefinedfield16',
                           'clientdefinedfield18']
            records[double_cols] = records[double_cols].apply(lambda x: x.str.strip().replace('', np.nan).fillna(0.0).astype(float))
        
        records['snap_dt'] = datetime.strptime(dt, '%Y%m%d')
        records_df = pa.Table.from_pandas(records)
        pq.write_table(records_df, output_path, use_deprecated_int96_timestamps=True)

def move_files_to_hdfs(filelist):
    """Move files to HDFS"""
    sc = spark.sparkContext
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path
    
    for local_file in filelist:
        file_name = local_file.split(sep='/')[-1]
        hdfs_sub_dir = local_file.split(sep='_')[2] + "/"
        hdfs_file = f"/prod/01559/app/RIE0/data_tde/COLDataFiles/{hdfs_sub_dir}{file_name}"
        
        fs.copyFromLocalFile(True, True, Path(local_file), Path(hdfs_file))

def remove_local_files(filelist):
    """Remove local files"""
    for file in filelist:
        os.remove(file)

def write_success_log(start_time, task_id, task_name, shell_name):
    """Write a successful log"""
    end = datetime.now()
    status = "Successfully completed"
    # Write log here
    pass

def err_handling(task_id, task_name, shell_name, err_log, record_count, recipients, start_time):
    """Error handling and logging"""
    # Handle errors and log error messages here
    pass
