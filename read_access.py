import pyodbc
import pandas as pd
import datetime as dt

s3_bucket = "s3://datahub-datawarehouse-dev-bucket"
s3_prefix = "/raw/door_access/"

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
