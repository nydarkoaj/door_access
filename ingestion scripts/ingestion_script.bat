@echo off
:: change to project directory
cd/d C:\Users\User\OneDrive - AmaliTech gGmbH\Documents\door_access-main\door_access-main

:: Run the python script with virtual env kernel and write logs (stdout and stderr) to a log file
door_access_env\Scripts\python.exe door_access_accra.py>>door_access_pipeline.log 2>&1

:: Exit
exit