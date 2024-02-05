ETL Process Documentation
This document outlines the functionalities of the various functions within the etl.py script, which orchestrates an Extract, Transform, Load (ETL) process for handling and processing data files.

Function Descriptions
check_mark_files()
Purpose: Verifies the presence of mark files in specified directories.

Details: This function checks for .mrk files in both the Bread and Katabat directories to ensure that all expected data files are present before beginning the ETL process. It logs the count of these files and validates against an expected number. If the actual count is less than expected, the function raises an exception, indicating a potential issue with data delivery.

copy_pgp_files()
Purpose: Copies PGP files from source directories to a local working directory for processing.

Details: It iterates through the PGP files located in the Bread and Katabat source directories, copying each file to a designated local directory (filepath_data). The function logs the name of each file copied and handles any exceptions encountered during the copy process, ensuring that all necessary data files are ready for subsequent processing steps.

generate_parquet_files()
Purpose: Transforms PGP files into Parquet format for efficient storage and querying.

Details: This function processes each PGP file, applying necessary data transformations and then converting the data into Parquet format. Special handling is provided for QueueExport files due to their unique structure. The process includes cleaning bad data, formatting according to predefined schemas, and generating Parquet files in a specified directory. Each step of the process is logged for audit and troubleshooting purposes.

move_parquet_to_hdfs()
Purpose: Moves generated Parquet files to Hadoop Distributed File System (HDFS) for distributed storage.

Details: After Parquet files are generated, this function transfers them from the local directory to HDFS. It leverages PySpark's SparkContext to interact with HDFS, ensuring that data is stored in a manner that supports scalable processing and analysis. The function handles the file transfer process, logging each file moved and managing any exceptions that occur.

delete_copied_pgp_files()
Purpose: Cleans up by deleting the PGP files copied to the local working directory after processing.

Details: To maintain a clean working environment and manage disk space efficiently, this function deletes all PGP files that were copied to the local directory (filepath_data) once they have been successfully processed into Parquet format and moved to HDFS. It logs the deletion of each file, ensuring transparency in the cleanup process.

log_processing_status()
Purpose: Logs the completion status of the ETL process.

Details: This function is called at the end of the ETL process to log a message indicating successful completion. It serves as a simple notification mechanism within the logging system to mark the end of the process.

daily_load()
Purpose: Orchestrates the entire ETL workflow.

Details: This is the main function that orchestrates the execution of the ETL process. It sequentially calls the other functions in the script, managing the flow from initial checks of mark files, through the copying and processing of PGP files, to the final cleanup and logging steps. It includes error handling to catch and log any issues that arise during the process, ensuring robustness and reliability of the ETL workflow.


Data Processing Utilities Documentation
This document describes the functionalities of the utility processing functions defined in data_processing_utils.py, outlining their roles in the data processing workflow and their integration within the ETL process orchestrated by etl.py.

Utility Function Descriptions
process_queue_export(file_path)
Purpose: Processes QueueExport files, transforming them into a structured DataFrame format suitable for Parquet conversion.

Details: This function reads a CSV file using a predefined schema, adds a record type, and formats date columns. It's specifically designed to handle the unique format of QueueExport files.

Used by ETL Function: generate_parquet_files()

remove_baddata(file_path)
Purpose: Cleans specific problematic data patterns in files before processing.

Details: Targets and corrects known bad data strings that could cause issues during data loading or transformation, replacing them with corrected versions.

Used by ETL Function: generate_parquet_files() (specifically for AccountPlacementImportFile processing)

initial_formatting(file_path)
Purpose: Performs initial formatting on data files to standardize their structure for further processing.

Details: Reads a file and performs initial splitting and structuring based on predefined rules and schemas. It identifies file names and snapshot dates for record grouping.

Used by ETL Function: generate_parquet_files()

process_subrecords(df, file_name, record_list, dt)
Purpose: Processes and partitions data files into subrecords, applying schemas and converting them to Parquet format.

Details: This function takes a DataFrame and iterates through its records, applying the appropriate schema based on the file type and record type. It generates Parquet files for each subset of data.

Used by ETL Function: generate_parquet_files()

move_file_hdfs(filelist)
Purpose: Moves generated Parquet files from the local filesystem to HDFS for distributed storage.

Details: Utilizes PySpark and Hadoop's FileSystem API to transfer files from the local directory to HDFS, supporting large-scale data storage and processing.

Used by ETL Function: move_parquet_to_hdfs()

remove_local_file(filelist)
Purpose: Deletes local files that have been successfully processed and moved to their final destination.

Details: Cleans up the working directory by removing files that are no longer needed, freeing up disk space and maintaining a tidy processing environment.

Used by ETL Function: delete_copied_pgp_files()
