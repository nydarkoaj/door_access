import os
import pandas as pd
import logging
import json
from typing import List, Tuple, Dict

# === Setup Logging === #
logging.basicConfig(
    filename='remote_days.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
EXCLUDED_NAMES = [
    "book before use", 
    "open to all & trainees / patrick amenuku", 
    "focus room / meeting room", 
    "n/a"
]

SNAPSHOT_FILE = "last_input_snapshot.json"

def load_excel_file(file_path: str) -> pd.ExcelFile:
    try:
        logging.info(f"Loading Excel file from: {file_path}")
        return pd.ExcelFile(file_path)
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        raise

def extract_day_name_pairs(xlsx: pd.ExcelFile) -> List[Tuple[str, str]]:
    results = []
    try:
        for sheet_name in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)
            data = df.values.tolist()

            for i, row in enumerate(data):
                if isinstance(row[0], str) and row[0].strip().lower() == "day":
                    for day_idx, day_row in enumerate(data[i + 1:i + 6]):
                        if day_idx >= len(WEEKDAYS):
                            break
                        day_name = WEEKDAYS[day_idx]
                        for col_idx in range(1, len(day_row)):
                            name = day_row[col_idx]
                            if (
                                pd.notna(name)
                                and isinstance(name, str)
                                and name.strip()
                                and name.strip().lower() not in EXCLUDED_NAMES
                            ):
                                results.append((day_name, name.strip()))
        logging.info("Successfully extracted (day, name) pairs.")
        return results
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def compute_remote_days(data: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    try:
        df_presence = pd.DataFrame(data, columns=["day", "name"])
        unique_names = df_presence["name"].unique()
        final_data = []

        for name in unique_names:
            present_days = df_presence[df_presence["name"] == name]["day"].unique().tolist()
            remote_days = [day for day in WEEKDAYS if day not in present_days]

            final_data.append({
                "name": name,
                "remote_day_one": remote_days[0] if len(remote_days) > 0 else "",
                "remote_day_two": remote_days[1] if len(remote_days) > 1 else ""
            })

        logging.info("Successfully computed remote days.")
        return final_data
    except Exception as e:
        logging.error(f"Error computing remote days: {e}")
        raise

def save_to_csv(final_data: List[Dict[str, str]], output_path: str):
    try:
        output_df = pd.DataFrame(final_data, columns=["name", "remote_day_one", "remote_day_two"])
        output_df.sort_values(by=["name", "remote_day_one", "remote_day_two"]).to_csv(output_path, index=False)
        logging.info(f"Data written to CSV: {output_path}")
        print(f"Output written to '{output_path}'")
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")
        raise

def load_last_snapshot() -> List[Tuple[str, str]]:
    if not os.path.exists(SNAPSHOT_FILE):
        return []
    try:
        with open(SNAPSHOT_FILE, "r") as f:
            data = json.load(f)
            # Convert loaded lists back to tuples
            return [tuple(item) for item in data]
    except Exception as e:
        logging.warning(f"Failed to load snapshot: {e}")
        return []

def save_snapshot(data: List[Tuple[str, str]]):
    try:
        with open(SNAPSHOT_FILE, "w") as f:
            # Convert tuples to lists for JSON serialization
            json.dump([list(item) for item in sorted(data)], f, indent=2)
        logging.info("Snapshot of source data saved.")
    except Exception as e:
        logging.error(f"Failed to save snapshot: {e}")

def main():
    try:
        downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
        file_path = os.path.join("data", "leave_data.xlsx")
        output_file = "remote_days_output2.csv"

        xlsx = load_excel_file(file_path)
        day_name_pairs = extract_day_name_pairs(xlsx)

        # Check if the source data has changed compared to the last snapshot
        last_snapshot = load_last_snapshot()
        if sorted(last_snapshot) == sorted(day_name_pairs):
            logging.info("No change in Excel source data. Skipping processing.")
            print("No changes detected in Excel source. Skipping.")
            return

        # Proceed with processing
        final_data = compute_remote_days(day_name_pairs)

        # Save the snapshot for future comparisons
        save_snapshot(day_name_pairs)

        save_to_csv(final_data, output_file)

    except Exception as e:
        logging.critical(f"Unhandled exception in main: {e}")
        print("An error occurred. Check 'remote_days.log' for details.")

if __name__ == "__main__":
    main()
