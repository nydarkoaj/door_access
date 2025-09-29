# ========== IMPORTS ==========
import pyodbc
import pandas as pd
import datetime as dt
import logging
from typing import Dict, List
import os
import json
import boto3
from dotenv import load_dotenv
from io import BytesIO,StringIO
import sys
# from get_all_logs import export_from_zk_access

load_dotenv()

s3_bucket_1 = os.getenv("BUCKET_NAME_1")
s3_client_1 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID_1"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_1"),
    region_name=os.getenv("AWS_REGION_1")
)

# Account 2
s3_bucket_2 = os.getenv("BUCKET_NAME_2")
s3_client_2 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID_2"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_2"),
    region_name=os.getenv("AWS_REGION_2")
)
s3_prefix = "raw/door-access-data/Kumasi"
s3_log_folder_path = "raw/door-access-data/logs/"
json_state_file = "state_file.json"


# #========== AWS S3 CONFIGURATION ==========
# # s3_bucket = os.getenv("BUCKET_NAME")
# s3_prefix = "raw/door-access-data/Kumasi"
# # access_key = os.getenv("AWS_ACCESS_KEY_ID")
# # secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
# # region = os.getenv("AWS_REGION")
# # s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
# s3_log_folder_path = "raw/door-access-data/logs/"
# json_state_file = "state_file.json"


# ========== CONFIGURATION ==========
MDB_FILE =r'C:\ZKTeco\ZKAccess3.5\Access.mdb'
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
USER_COLUMNS = ['USERID', 'name', 'lastname', 'email', 'DEFAULTDEPTID','CardNo']
EVENTLOG_COLUMNS = ['id', 'time', 'device_name', 'state', 'event_type', 'event_point_name']
DEPARTMENT_COLUMNS = ['DEPTID','DEPTNAME','SUPDEPTID']

current_time = dt.datetime.now().strftime("%Y_%m_%d_%H_%S")
current_date = dt.datetime.now().strftime("%Y-%m-%d")


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
    # return df[df['logtime_year'] >= 2024]
    current_year = dt.datetime.now().year
    return df[(df['logtime_year'] >= 2024) & (df['logtime_year'] <= current_year)]


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
        'DEFAULTDEPTID': 'deptid',
        'CardNo' : 'card number'
    }, inplace=True)
    return df

def clean_eventlog_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the event logs."""
    df = df[EVENTLOG_COLUMNS].copy()
    df.rename(columns={
        'id': 'logid',
        'time': 'logtime',
        'device_name': 'device name'
    }, inplace=True)
    df['logtime'] = pd.to_datetime(df['logtime'])
    df['logtime_year'] = df['logtime'].dt.year
    # return df[df['logtime_year'] >= 2024]
    current_year = dt.datetime.now().year
    return df[(df['logtime_year'] >= 2024) & (df['logtime_year'] <= current_year)]

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
    log_events_df = eventlog_df.merge(
        checkin_df,
        on='logtime',
        how='left',
        suffixes=('_eventlog', '_checkin'))

    merged_df = pd.merge(employee_df, log_events_df, on = 'userid', how='inner')

    # Adding additional required columns
    merged_df['verify type'] = ''
    merged_df['in/out status'] = ''
    merged_df['event description'] = ''
    merged_df['remarks'] = ''

    # Changing column names
    merged_df.rename(columns={
        'userid': 'personnel id',
        'logtime': 'date and time',
        'event_point_name': 'event point',
        'firstname': 'first name',
        'lastname': 'last name'

    }, inplace=True) 

    # Reorer columns
    final_columns = [
        'date and time',
        'personnel id',
        'first name',
        'last name',
        'card number',
        'device name',
        'event point',
        'verify type',
        'in/out status',
        'event description',
        'remarks'
    ]
    merged_df = merged_df[final_columns]


    
    # Update dict with merged dataframes
    # update_dict= {'employee':employee_df,'log_events':log_events_df}
    # dfs.update(update_dict)
    print("dataframe has", len(merged_df),"rows")
    return merged_df

# Initial grouping function to change the DataFrame to a dict with year_month as key
def group_by_year_month_1(df: pd.DataFrame,year_month: str) -> Dict[str, pd.DataFrame]:
    """Group the DataFrame by year and month."""
    df['year_month'] = df['date and time'].dt.to_period('M')
    df['year_month'] = df['year_month'].astype(str)
    logger.info(f"skipping dfs greater than or equal to {year_month} since it has already been processed...")
    filtered_df = df[df['year_month'] >= year_month]
    grouped = filtered_df.groupby('year_month')
    return {name: group.drop(columns='year_month') for name, group in grouped}


def upload_to_s3(dfs: Dict[str, pd.DataFrame]) -> None:
    """Upload the DataFrame to both S3 buckets as CSV files."""
    for name, df in dfs.items():
        if not df.empty:
            name = name.replace("-", "_")
            year = name[:4]
            file_key = f"{s3_prefix}/year={year}/kumasi_attendance_{name}.csv"

            # Upload to first bucket
            logger.info(f"Uploading {name} to {s3_bucket_1}/{file_key}")
            s3_client_1.put_object(
                Bucket=s3_bucket_1,
                Key=file_key,
                Body=df.to_csv(index=False)
            )

            # Upload to second bucket
            logger.info(f"Uploading {name} to {s3_bucket_2}/{file_key}")
            s3_client_2.put_object(
                Bucket=s3_bucket_2,
                Key=file_key,
                Body=df.to_csv(index=False)
            )

        else:
            logger.warning(f"DataFrame for {name} is empty. Skipping upload.")

# def upload_to_s3(dfs: Dict[str, pd.DataFrame]) -> None:
#     """Upload the DataFrame to S3 as CSV files."""
#     for name, df in dfs.items():
#         if not df.empty:
#             # df.copy = df.drop(columns='year_month', inplace=True)
#             # replace hypehns with underscores in the name
#             name = name.replace('-', '_')
#             year = name[:4]
#             # print sample path in s3
#             logger.info(f"Uploading {name} to S3 at {s3_bucket}/{s3_prefix}/year={year}/kumasi_attendance_{name}.csv")
#             s3_client.put_object(
#                 Bucket=s3_bucket,
#                 Key=f"{s3_prefix}/year={year}/kumasi_attendance_{name}.csv",
#                 Body=df.to_csv(index=False)
#             )
#         else:    
#             logger.warning(f"DataFrame for {name} is empty. Skipping upload.")


def save_csvs_locally(dfs: Dict[str, pd.DataFrame]) -> None:
    """Save the DataFrame to local CSV files."""
    for name, df in dfs.items():
        if not df.empty:
            # replace hyphens with underscores in the name
            name = name.replace('-', '_')
            year = name[:4]
            # print sample path in s3
            logger.info(f"Saving {name} to local path ./data_exports/kumasi_attendance_{name}.csv")
            df.to_csv(f"./data_exports/kumasi_attendance_{name}.csv", index=False)
        else:
            logger.warning(f"DataFrame for {name} is empty. Skipping save.")

def get_max_year_month(dfs: Dict[str, pd.DataFrame]):
    """write the latest year-month date to a csv state file, for incremental loading"""
    #unpack the keys (year month) into a list and get the max value
    keys = [*dfs.keys()]
    last_processed_month = max(keys)
    print('last proccessed year month is', last_processed_month)
    # write the last processed month into a file --, with two other columns,processedtime, and max_datetimestamp.
    with open (json_state_file,'w') as f:
        json.dump({'door_access_pipeline':{'df_current_month':last_processed_month, 'df_process_timestamp':current_time}},f,indent=2)

def get_last_year_month(json_state_file:dict)-> str:
    logger.info("loading state file...")
    with open (json_state_file,'r') as f:
        data = json.load(f)
        print(data)
        last_year_month = data.get('door_access_pipeline',{}).get('df_current_month')
        return last_year_month


# ========== MAIN PIPELINE ==========
def main() -> pd.DataFrame:
    """Orchestrate the full data extraction, cleaning, and merging process."""
    logger.info("Starting data ingestion pipeline...")
    logger.info(f"Current timestamp: {current_time}")

    # export_from_zk_access()

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
    df = merge_data(clean_user_df, clean_checkin_df, clean_eventlog_df, clean_department_df)

    year_month = get_last_year_month(json_state_file)    
    df_groups = group_by_year_month_1(df,year_month)
    print("Grouped data has", len(df_groups), "groups")

    logger.info("saving the grouped data as CSV files...")
    save_csvs_locally(df_groups)

    logger.info("uploading grouped data to S3...")
    upload_to_s3(df_groups)

    logger.info("saving metadata to state file...")
    get_max_year_month(df_groups)


    if df_groups:
        logger.info("Data ingestion pipeline completed successfully.")
    else:
        logger.warning("Data ingestion pipeline completed with no data.")   

    return df_groups # Return the first group as the final dataset

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    final_dataset = main()
    # print(final_dataset)        

