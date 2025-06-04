import pandas as pd
from thefuzz import fuzz, process
import re
import logging
from typing import Optional, List

MATCH_THRESHOLD = 70

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("merge_log.log"),
        logging.StreamHandler()
    ]
)


def load_data(file_path: str, file_type: str) -> pd.DataFrame:
    """Loads CSV or XLSX file based on type."""
    try:
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xlsx':
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'xlsx'.")
        logging.info(f"Loaded {file_type.upper()} file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading file: {e}")
        raise

def clean_name(name: str) -> str:
    """Lowercase and remove extra spaces and non-letters."""
    name = str(name).strip().lower()
    name = re.sub(r'[^a-z\s]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name

def safe_str(value):
    return "None" if pd.isna(value) else str(value).strip()


def match_name(target_name: str, candidates: List[str]) -> Optional[str]:
    """Find the best fuzzy match with score >= threshold."""
    match, score = process.extractOne(target_name, candidates, scorer=fuzz.ratio)
    return match if score >= MATCH_THRESHOLD else None


def process_and_merge(remote_df: pd.DataFrame, zk_df: pd.DataFrame, emp_df: pd.DataFrame) -> pd.DataFrame:
    try:
        for col in ['name', 'block', 'remote_day_one', 'remote_day_two']:
            if col not in remote_df.columns:
                raise KeyError(f"Missing column in remote_days: {col}")
        for col in ['first name', 'last name', 'card number']:
            if col not in zk_df.columns:
                raise KeyError(f"Missing column in zkaccess_data: {col}")
        for col in ['name', 'office', 'user_id']:
            if col not in emp_df.columns:
                raise KeyError(f"Missing column in employee_data: {col}")

        remote_df['clean_name'] = remote_df['name'].apply(clean_name)
        zk_df['clean_name'] = (zk_df['first name'].fillna('') + ' ' + zk_df['last name'].fillna('')).apply(clean_name)
        emp_df = emp_df[emp_df['office'].str.lower() == 'accra'].copy()
        emp_df['clean_name'] = emp_df['name'].apply(clean_name)

        remote_lookup = dict(zip(remote_df['clean_name'], remote_df.to_dict('records')))
        zk_lookup = dict(zip(zk_df['clean_name'], zk_df.to_dict('records')))

        combined_records = []
        unmatched_remote = []

        for _, emp_row in emp_df.iterrows():
            emp_name = emp_row['clean_name']

            top_matches = process.extract(emp_name, list(remote_lookup.keys()), scorer=fuzz.ratio, limit=3)

            matched_remote_name, score = top_matches[0] if top_matches else (None, 0)

            if score < MATCH_THRESHOLD:
                unmatched_remote.append({
                    "employee_id": emp_row.get("user_id"),
                    "employee_name": emp_row.get("name"),
                    "top_matches": [
                        {"name": name, "score": scr} for name, scr in top_matches
                    ]
                })
                matched_remote_name = None
            matched_remote_name = matched_remote_name if score >= MATCH_THRESHOLD else None
            matched_remote = remote_lookup.get(matched_remote_name) if matched_remote_name else {}

            matched_zk_name = match_name(emp_name, list(zk_lookup.keys()))
            zk_row = zk_lookup.get(matched_zk_name) if matched_zk_name else {}

            try:
                card_number = zk_row.get("card number")
                card_id = int(float(card_number)) if pd.notna(card_number) else None
            except (ValueError, TypeError):
                logging.warning(f"Invalid card number for {emp_row['name']}: {card_number}")
                card_id = None

            record = {
                "employee_id": str(emp_row.get("user_id") or "None"),
                "card_id": str(card_id) if card_id is not None else "0",
                "name": str(emp_row.get("name") or "None"),
                "office": safe_str(matched_remote.get("block")),
                "remote_day_1": safe_str(matched_remote.get("remote_day_one")),
                "remote_day_2": safe_str(matched_remote.get("remote_day_two")),
                "location": safe_str(emp_row.get("office")),
            }


            combined_records.append(record)

        combined_df = pd.DataFrame(combined_records)

        if unmatched_remote:
            import json
            with open("unmatched_remote_employees.json", "w") as f:
                json.dump(unmatched_remote, f, indent=4)
            logging.info(f"Saved {len(unmatched_remote)} unmatched remote entries with suggestions to 'unmatched_remote_employees.json'")


        logging.info(f"Matched {len(combined_df)} Accra employees to remote_days and zkaccess data.")
        return combined_df

    except Exception as e:
        logging.error(f"Error during matching and merging: {e}")
        raise



def main():
    try:
        employee_data = load_data("Final_Merger/data/employee_data.csv", "csv")
        remote_days_data = load_data("Final_Merger/data/remote_days.csv", "csv")
        zkaccess_data = load_data("Final_Merger/data/zkaccess_data.xlsx", "xlsx")

        result_df = process_and_merge(remote_days_data, zkaccess_data, employee_data)
        logging.info(f"Final merged data contains {len(result_df)} records.")

        result_df.to_csv("Remote Days (Accra).csv", index=False)
        logging.info("Merged data saved to 'Remote Days (Accra).csv'")

    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()
