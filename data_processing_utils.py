# data_processing_utils.py
from config import *
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def process_queue_export(file_path):
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

def remove_baddata(file_path):
    problemString1 = "\"syedasimhussain|@gmail.com\""
    correctString1 = "syedasimhussain@gmail.com"
    problemString2 = "Milliken Bridlewood Vet Clinic | 宠物医院, 2770 Kennedy Rd, Scarborough, ON M1T 3J2"
    correctString2 = "Milliken Bridlewood Vet Clinic , 宠物医院, 2770 Kennedy Rd, Scarborough, ON M1T 3J2"
    try:
        with open(file_path, "r") as file:
            csvReader = file.read()
        csvReader = csvReader.replace(problemString1, correctString1).replace(problemString2, correctString2)
        with open(file_path, "w") as file:
            file.write(csvReader)
    except Exception as e:
        print(f"Error removing bad data: {e}")

def initial_formatting(file_path):
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
    prefix = prefix_dict.get(file_name)
    for rec in record_list:
        records = df[df['recordtype'] == rec]
        schema = bread_schemas.get(f'{prefix}{rec.lower()}')
        output_path = f'{src}/Data/CMC_{file_name}_{rec}_{dt}.parquet'
        schema = [col for col in schema if col not in ['id', 'recordtype', 'rbcsource', 'location', 'responsibiltytransit', 'filegenerationdate', 'filegenerationtime', 'dataeffectivedate', 'directionind']]
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
            records[col] = pd.to_datetime(records[col], errors='coerce', format='%Y%m%d%H%M%S').fillna('')
        records['snap_dt'] = datetime.strptime(dt, '%Y%m%d')
        records_df = pa.Table.from_pandas(records)
        pq.write_table(records_df, output_path, use_deprecated_int96_timestamps=True)

def move_file_hdfs(filelist):
    # Assuming the use of PySpark's SparkContext or a similar setup to interact with HDFS
    sc = spark.sparkContext
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path
    for local_file in filelist:
        file_name = local_file.split('/')[-1]
        hdfs_sub_dir = local_file.split('_')[2] + "/"
        hdfs_file = f"/prod/01559/app/RIE0/data_tde/COLDataFiles/{hdfs_sub_dir}{file_name}"
        fs.copyFromLocalFile(True, True, Path(local_file), Path(hdfs_file))

def remove_local_file(filelist):
    for file in filelist:
        os.remove(file)
