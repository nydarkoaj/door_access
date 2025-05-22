
# Remote Day Processor - Setup Instructions


This tool downloads an Excel file from SharePoint and analyzes remote working days.

-------------
Folder Structure:
-------------
```
RemoteDayProcessor/
│
├── run_all.bat
├── README.txt
│
├── scripts/
│   ├── download_excel.ps1        
│   └── process_remote_days.py    
│
└── data/
    └── leave_data.xlsx         
```     
-------------
Prerequisites:
-------------
1. **PowerShell Requirements**:
   - Install Microsoft Graph PowerShell SDK:
     Open PowerShell as admin and run:
     ```
     Install-Module Microsoft.Graph -Scope CurrentUser
     ```

   - Login at least once manually to grant permissions(not as admin):
     ```
     Connect-MgGraph -Scopes "Sites.Read.All", "Files.Read.All"
     ```

2. **Python Requirements**:
   - Python 3.x must be installed and added to your PATH
   - Required packages:
     ```
     pip install pandas
     ```

-------------
How to Run:
-------------
1. Open a terminal or double-click `run_all.bat`
2. It will:
   - Use PowerShell to download `leave_data.xlsx` to the `data/` folder
   - Run Python script to compute remote days and export a CSV

-------------
Output:
-------------
- The output file will be saved as `remote_days_output2.csv`
- A log file `remote_days.log` will be created to track events
- A snapshot file `last_input_snapshot.json` helps detect repeated runs

-------------
Notes:
-------------
- Make sure to update the file paths inside `download_excel.ps1` and `process_remote_days.py` to point to the correct `data\leave_data.xlsx` location.
