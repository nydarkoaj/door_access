# 📂 Door Access Data Pipeline

This project extracts, cleans, merges, and uploads door access data from a Microsoft Access (`.mdb`) database to AWS S3 for downstream analytics.

---

## 🚀 Project Structure

```
├── door_access_pipeline.py  # Main pipeline script
├── .env                     # Environment variables (AWS credentials)
├── README.md                 # Project documentation
└── requirements.txt          # Python dependencies (optional to create)
```

---

## ⚙️ Setup Instructions

1. **Clone the Repository**  
   ```bash
   git clone https://github.com/nydarkoaj/door_access.git
   cd door_access
   ```

2. **Set Up Environment Variables**  
   Create a `.env` file in the project root with the following:
   ```
   AWS_ACCESS_KEY_ID=your-access-key-id
   AWS_SECRET_ACCESS_KEY=your-secret-access-key
   AWS_REGION=your-aws-region
   ```

3. **Install Dependencies**  
   (Recommended to use a virtual environment)
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Microsoft Access Driver**  
   - Ensure that the **Microsoft Access ODBC Driver** is installed on your machine.
   - If using `.accdb` format, you may need the **Microsoft Access Database Engine**.

---

## 🛠 How to Run the Pipeline

```bash
python door_access_pipeline.py
```

- The script will:
  - Connect to the Access database
  - Extract and clean the data
  - Merge datasets
  - Upload the final CSV to an AWS S3 bucket under `raw/door_access/`

---

## 📦 Key Components

- **Database Connection**: Connects to Access database using `pyodbc`
- **Data Cleaning**: Standardizes columns, formats timestamps, filters data from 2024 onward
- **AWS S3 Upload**: Pushes processed CSV to S3 with timestamped filenames

---

## 🧹 Cleaning Logic Summary

| Table           | Columns Used                                              | Notes                         |
|-----------------|------------------------------------------------------------|-------------------------------|
| USERINFO        | USERID, name, lastname, email, DEFAULTDEPTID               | Rename, minimal transformation |
| CHECKINOUT      | USERID, CHECKTIME, LOGID                                   | Filter by year >= 2024         |
| acc_monitor_log | id, time, device_name, state, event_type, event_point_name  | Filter by year >= 2024         |

---

## ⚠️ Important Notes

- Be sure your `.env` file is **NOT pushed** to GitHub (add it to `.gitignore`).
- Ensure the Access database file (`access.mdb`) path is correctly set in the script.
- Requires network access to AWS S3.

---

## 🙌 Contributions

- If you add new tables, update the `TABLE_MAPPING` dictionary.
- If new fields are needed, modify the cleaning functions accordingly.

---

## 📄 License

This project is licensed internally and intended for team collaboration.

---

## ✅ Quick Start Checklist

- [ ] Install dependencies
- [ ] Add `.env` credentials
- [ ] Verify Access database path
- [ ] Run `door_access_pipeline.py`
- [ ] Check S3 for the uploaded file!

---
