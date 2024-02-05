# config.py
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
from datetime import datetime, timedelta
import sys
import shutil
import logging
import traceback
from pyspark.sql import SparkSession

# Suppress warnings
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None

# Path configurations
src = '/users/prieappcol/BreadReport'
filepath_bread = '/users/prieappcol/Bread/'
filepath_katbat = '/users/prieappcol/Katabat/'
filepath_data = '/users/prieappcol/BreadReport/Data/'

# Logging configuration
today = datetime.today().strftime("%Y-%m-%d")
syncday = str(os.getenv("syncday"))
logging.basicConfig(filename='{0}/Log/User_Log/BreadIngestion_Daily_{1}.log'.format(src, today), level=logging.INFO)

# Spark session initialization
spark = SparkSession.builder.appName("BreadIngestion")\
    .config("spark.sql.parquet.binaryAsString","true")\
    .config("spark.sql.execution.arrow.enabled","true")\
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
    .config("spark.dynamicAllocation.enabled","true")\
    .config("spark.shuffle.service.enabled","true")\
    .config("spark.dynamicAllocation.minExecutors","2")\
    .config("spark.dynamicAllocation.initialExecutors","2")\
    .config("spark.executor.instances","4")\
    .config("spark.dynamicAllocation.maxExecutors","12")\
    .config("spark.driver.memory","1g")\
    .enableHiveSupport()\
    .getOrCreate()   

# Prefix dictionary for file naming
prefix_dict = {
    'AcctActReturnFile': 'cmc_aar_',
    'AgentReturnFile': 'cmc_atr_',
    'WorkflowReturnFile': 'cmc_wfr_',
    'ProcessedPmtsImportFile': 'pmt_',
    'PmtTransExportFile': 'pmt_',
    'AcctPlacementFile': 'apf_'
}

# Load schema configurations
with open('{}/Data/bread_schemas.json'.format(src),'r') as fp:
    bread_schemas = json.load(fp)
