import pyodbc
import pandas as pd
import datetime as dt
import logging
from typing import Dict, List
import os
import boto3
from dotenv import load_dotenv

load_dotenv()


s3_bucket = "s3://datahub-datawarehouse-dev-bucket"
s3_prefix = "/raw/door_access/"
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION")
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)


buckets = s3_client.list_buckets()
for bucket in buckets['Buckets']:
    print(bucket['Name'])

# ========== CONFIGURATION ==========
MDB_FILE = r'C:\Users\NanaYawDarko\door_access_accra\access.mdb'
CONN_STR = (
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
    rf'DBQ={MDB_FILE};'
)

NEEDED_TABLES = [
    'USERINFO', 'CHECKINOUT', 'DEPARTMENTS', 'Machines', 'acc_monitor_log', 'acc_door', 'action_log'
]

TABLE_MAPPING = {
    'checkin': 'CHECKINOUT',
    'user': 'USERINFO',
    'eventlog': 'acc_monitor_log'
}

CHECKIN_COLUMNS = ['USERID', 'CHECKTIME', 'LOGID']
USER_COLUMNS = ['USERID', 'name', 'lastname', 'email', 'DEFAULTDEPTID']
EVENTLOG_COLUMNS = ['id', 'time', 'device_name', 'state', 'event_type', 'event_point_name']

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
    eventlog_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge cleaned user, check-in, and event log DataFrames."""
    user_checkin_df = user_df.merge(checkin_df, on='userid', how='left')
    final_df = user_checkin_df.merge(
        eventlog_df,
        on='logtime',
        how='left',
        suffixes=('_checkin', '_eventlog')
    )
    return final_df

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

    logger.info("Merging datasets...")
    final_df = merge_data(clean_user_df, clean_checkin_df, clean_eventlog_df)

    logger.info("Data processing completed successfully.")
    return final_df

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    final_dataset = main()
    logger.info("Sample of processed data:")
    logger.info("\n%s", final_dataset.head())
