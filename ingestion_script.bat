@echo off
:: change to project directory
cd/d "C:\Users\HP\Desktop\door_access"

:: run get all logs script
venv\Scripts\python.exe get_all_logs.py >> get_all_logs.log 2>&1

:: Run the python script with virtual env kernel and write logs (stdout and stderr) to a log file
venv\Scripts\python.exe door_access_kumasi.py>>door_access_pipeline.log 2>&1

:: Exit
exit