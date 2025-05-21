@echo off
:: Turn off command echoing to make output cleaner

:: Display a message
echo Running PowerShell script to download Excel file...

:: Run the PowerShell script to download the Excel file from SharePoint
:: -ExecutionPolicy Bypass allows running the script even if script execution is restricted
:: -File specifies the path to the .ps1 script
powershell -ExecutionPolicy Bypass -File ""scripts\download_excel.ps1"

:: Display a message
echo Running Python script to process the Excel file...

:: Run the Python script that processes the downloaded Excel file
python "scripts\process_remote_days.py"

:: Notify the user all tasks are completed
echo All tasks completed.

:: Keep the window open so you can see any messages or errors
pause
