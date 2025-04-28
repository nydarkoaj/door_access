import pyodbc
import pandas as pd
import datetime as dt
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

# Path to your .mdb file
mdb_file = r'C:\Users\NanaYawDarko\door_access_accra\access.mdb'
conn_str = (
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
    rf'DBQ={mdb_file};'
)

# Connect to the database
conn = pyodbc.connect(conn_str)

# Get table names
cursor = conn.cursor()
tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
print("Tables in the database: ",tables)

# filter to include tables we need
needed_tables = ['USERINFO','CHECKINOUT','DEPARTMENTS','Machines','acc_monitor_log','acc_door','action_log']

print("Tables to extract:",needed_tables)

datestring = dt.datetime.now().strftime("%Y_%m_%d_%H")
print("Current date and time:", datestring)

# Read non empty tables into DataFrames
for table in needed_tables:
    df = pd.read_sql(f"SELECT * FROM [{table}]", conn)
    if df.empty:
        continue
    else:
        # save to csv
        print(f"Exporting {table} to CSV")
        
        df.to_csv(f'data_exports\{table}.csv', index=False)

cursor.close()
conn.close()
