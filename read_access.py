# ========== IMPORTS ==========
import pyodbc
import pandas as pd
import datetime as dt
import logging
from typing import Dict, List
import os
import boto3
from dotenv import load_dotenv
from io import BytesIO
import time


load_dotenv()

#========== AWS S3 CONFIGURATION ==========
s3_bucket = "datahub-datawarehouse-dev-bucket"
s3_prefix = "raw/door_access/"
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION")
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)




# ========== CONFIGURATION ==========
MDB_FILE = r'C:\Users\NanaYawDarko\door_access_accra\access.mdb'
CONN_STR = (
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
    rf'DBQ={MDB_FILE};'
)



NEEDED_TABLES = [
    'USERINFO', 'CHECKINOUT', 'DEPARTMENTS', 'acc_monitor_log'
]

TABLE_MAPPING = {
    'checkin': 'CHECKINOUT',
    'user': 'USERINFO',
    'eventlog': 'acc_monitor_log',
    'departments': 'DEPARTMENTS'
}

CHECKIN_COLUMNS = ['USERID', 'CHECKTIME', 'LOGID']
USER_COLUMNS = ['USERID', 'name', 'lastname', 'email', 'DEFAULTDEPTID']
EVENTLOG_COLUMNS = ['id', 'time', 'device_name', 'state', 'event_type', 'event_point_name']
DEPARTMENT_COLUMNS = ['DEPTID','DEPTNAME','SUPDEPTID']

# ========== SETUP LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== DATABASE CONNECTIONS ==========
def get_connection() -> pyodbc.Connection:
    """Create a database connection."""
    return pyodbc.connect(CONN_STR)

def list_tables() -> List[str]:
    """List all table names from the Access database."""
    with get_connection() as conn:
        cursor = conn.cursor()
        tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
    logger.info(f"Tables in the database: {tables}")
    return tables

def load_data(tables_to_load: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """Load specified tables into pandas DataFrames."""
    dfs = {}
    with get_connection() as conn:
        for name, table in tables_to_load.items():
            logger.info(f"Loading table: {table}")
            dfs[name] = pd.read_sql(f"SELECT * FROM [{table}]", conn)
    return dfs

# ========== CLEANING FUNCTIONS ==========
def clean_checkin_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the check-in/out records."""
    df = df[CHECKIN_COLUMNS].copy()
    df.rename(columns={
        'USERID': 'userid',
        'CHECKTIME': 'logtime',
        'LOGID': 'logid'
    }, inplace=True)
    df['logtime'] = pd.to_datetime(df['logtime'])
    df['logtime_year'] = df['logtime'].dt.year
    return df[df['logtime_year'] >= 2024]

def clean_department_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the department information."""
    df = df[DEPARTMENT_COLUMNS].copy()
    df.rename(columns={
        'DEPTID': 'deptid',
        'DEPTNAME': 'deptname',
        'SUPDEPTID': 'supdeptid'
    }, inplace=True)
    return df

def clean_user_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the user information."""
    df = df[USER_COLUMNS].copy()
    df.rename(columns={
        'USERID': 'userid',
        'name': 'firstname',
        'email': 'Email',
        'DEFAULTDEPTID': 'deptid'
    }, inplace=True)
    return df

def clean_eventlog_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the event logs."""
    df = df[EVENTLOG_COLUMNS].copy()
    df.rename(columns={
        'id': 'logid',
        'time': 'logtime'
    }, inplace=True)
    df['logtime'] = pd.to_datetime(df['logtime'])
    df['logtime_year'] = df['logtime'].dt.year
    return df[df['logtime_year'] >= 2024]

# ========== MERGING FUNCTION ==========
def merge_data(
    user_df: pd.DataFrame,
    checkin_df: pd.DataFrame,
    eventlog_df: pd.DataFrame,
    department_df: pd.DataFrame
) -> Dict [str ,pd.DataFrame]:
    """Merge cleaned user and department to create employee_df, merge check-in and event log tables to create log_events_df.

        returns a dict with the merged dataframes and names as keys.

        employee_df: DataFrame containing user and department information.
        log_events_df: DataFrame containing check-in and event log information.
    
    """
    dfs = {}
    # Merge user and department dataframes
    employee_df  = user_df.merge(department_df, on='deptid', how='left')

    # Merge check-in and event log dataframes
    log_events_df = checkin_df.merge(
        eventlog_df,
        on='logid',
        how='left',
        suffixes=('_checkin', '_eventlog')
    )
    # Update dict with merged dataframes
    update_dict= {'employee_df':employee_df,'log_events_df':log_events_df}
    dfs.update(update_dict)
 
    return dfs


# ========== S3 UPLOAD FUNCTION ==========
def upload_all_dfs_to_s3(dfs: Dict[str, pd.DataFrame], bucket: str, prefix: str) -> None:
    # upload all dataframes to s3 as parquet files in the accra directory for my s3 path
    for name, df in dfs.items():
        s3_path = f"{prefix}{name}.parquet"
        try:
            bytes_buffer = BytesIO()
            df.to_parquet(bytes_buffer, index=False)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_path, Body=bytes_buffer.getvalue())
            logger.info(f"Uploaded {name} to s3://{bucket}/{s3_path}")
        except Exception as e:
            logger.error(f"Failed to upload {name} to S3: {e}")
        else:
            logger.info(f"Upload of {name} to S3 completed.")
# ========== MAIN PIPELINE ==========
def main() -> pd.DataFrame:
    """Orchestrate the full data extraction, cleaning, and merging process."""
    logger.info("Starting data ingestion pipeline...")
    current_time = dt.datetime.now().strftime("%Y_%m_%d_%H")
    logger.info(f"Current timestamp: {current_time}")

    logger.info("Listing tables in the database...")
    list_tables()

    logger.info("Loading required tables...")
    dfs = load_data(TABLE_MAPPING)

    logger.info("Cleaning datasets...")
    clean_checkin_df = clean_checkin_data(dfs['checkin'])
    clean_user_df = clean_user_data(dfs['user'])
    clean_eventlog_df = clean_eventlog_data(dfs['eventlog'])
    clean_department_df = clean_department_data(dfs['departments'])

    logger.info("Merging datasets...")
    final_dfs = merge_data(clean_user_df, clean_checkin_df, clean_eventlog_df, clean_department_df)

    logger.info("uploading employee + logevents datasets as parquet files to S3...")
    try :
        upload_all_dfs_to_s3(final_dfs, s3_bucket, s3_prefix)
    except Exception as e:
        logger.error(f"Failed to upload dataframes to S3: {e}")
   
    
    return final_dfs['employee_df']
        
    # s3_key = f"{s3_prefix}merged_data_{current_time}.csv"
    # try:
    #     # s3_client.upload_file(s3_path, s3_bucket, f"{s3_prefix}merged_data_{current_time}.csv")
    #     csv_buffer = StringIO()
    #     final_df.to_csv(csv_buffer, index=False)

    #     s3_client.put_object(Bucket=s3_bucket,Key=s3_key, Body=csv_buffer.getvalue())
    #     logger.info(f"File uploaded to s3://{s3_bucket}/{s3_key}")
    # except Exception as e:
    #     logger.error(f"Failed to upload file to S3: {e}")
    # else:
    #     logger.info("Data ingestion completed successfully.")
    #     return final_df

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    final_dataset = main()
    logger.info("\n%s", "Sample of processed data:")
    logger.info("\n%s", final_dataset.head())

#s3://datahub-datawarehouse-dev-bucket/raw/door_access/