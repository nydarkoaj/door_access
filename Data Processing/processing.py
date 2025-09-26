import pandas as pd
import os
import logging
from datetime import datetime
import re
from difflib import SequenceMatcher as SM

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_name(name):
    """Normalize name for matching: lower case, remove extra spaces, remove special chars."""
    if pd.isna(name):
        return ''
    name = str(name).lower().strip()
    name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with single
    name = re.sub(r'[^a-z\s]', '', name)  # Remove non-letter chars except space
    return name

def get_similarity_ratio(a, b):
    """Get similarity ratio between two strings."""
    return SM(None, a, b).ratio() * 100

def get_best_match(name, candidates, threshold=80):
    """Find the best matching candidate above threshold."""
    best_ratio = 0
    best_cand = None
    for cand in candidates:
        ratio = get_similarity_ratio(name, cand)
        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_cand = cand
    return best_cand, best_ratio

def assign_department(emp_id):
    """Assign department based on employee id pattern."""
    if pd.isna(emp_id):
        return pd.NA
    emp_id = str(emp_id).upper()
    if "OP" in emp_id:
        return "Operation"
    elif "SC" in emp_id:
        return "Service Center"
    elif "TC" in emp_id:
        return "Training Center"
    else:
        return ""
 

def combine_remote_days(data_dir, save=True):
    """Combine all remote days CSVs into one DF, standardize to 'floor'."""
    remote_dir = os.path.join(data_dir, 'remote_days')
    remote_files = [f for f in os.listdir(remote_dir) if f.endswith('.csv')]
    
    if not remote_files:
        logging.warning("No remote days files found.")
        return pd.DataFrame()
    
    dfs = []
    for file in remote_files:
        path = os.path.join(remote_dir, file)
        try:
            df = pd.read_csv(path)
            logging.info(f"Loaded remote days file: {file}")
            
            # Standardize to 'floor'
            if 'floor' in df.columns:
                df['office'] = df['floor']
            elif 'office' in df.columns:
                df['office'] = df['office']
            else:
                df['office'] = pd.NA
                logging.warning(f"No 'floor' or 'office' column in {file}")
            
            # Ensure remote_day columns up to 3, fill missing with NaN
            for i in range(1, 4):
                col = f'remote_day_{i}'
                if col not in df.columns:
                    df[col] = pd.NA
            
            # Normalize name for matching
            df['normalized_name'] = df['name'].apply(normalize_name)
            
            dfs.append(df)
        except Exception as e:
            logging.error(f"Error loading {file}: {e}")
    
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        if save:
            output_file = os.path.join(data_dir, "remote_combined.csv")
            combined.to_csv(output_file, index=False)
            logging.info(f"Combined remote data saved to {output_file}")
        return combined
    return pd.DataFrame()

def combine_zkaccess(data_dir, save=True):
    """Combine all zkaccess CSVs into one DF."""
    zk_dir = os.path.join(data_dir, 'zkaccess_export')
    zk_files = [f for f in os.listdir(zk_dir) if f.endswith('.csv')]
    
    if not zk_files:
        logging.warning("No zkaccess files found.")
        return pd.DataFrame()
    
    dfs = []
    for file in zk_files:
        path = os.path.join(zk_dir, file)
        try:
            df = pd.read_csv(path)
            logging.info(f"Loaded zkaccess file: {file}")
            
            # Create full name and normalized
            df['attendance_name'] = df['first name'].astype(str) + ' ' + df['last name'].astype(str)
            df['normalized_name'] = df['attendance_name'].apply(normalize_name)
            
            dfs.append(df)
        except Exception as e:
            logging.error(f"Error loading {file}: {e}")
    
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)

        if save:
            output_file = os.path.join(data_dir, "combined_zkaccess.csv")
            combined.to_csv(output_file, index=False)
            logging.info(f"Combined zkaccess data saved to {output_file}")
        return combined
    return pd.DataFrame()

def enhance_employee_data(employee_df, zk_df, remote_df):
    """Enhance employee DF with data from zk and remote using fuzzy matching."""
    # Normalize name in employee
    employee_df['normalized_name'] = employee_df['name'].apply(normalize_name)
    
    # Prepare new columns
    employee_df['personnel_id'] = pd.NA
    employee_df['attendance_name'] = pd.NA
    employee_df['office'] = pd.NA
    employee_df['remote_location'] = pd.NA
    employee_df['remote_name'] = pd.NA 
    for i in range(1, 4):
        employee_df[f'remote_day_{i}'] = pd.NA
    
    # Fuzzy match with zkaccess for personnel_id and attendance_name
    if not zk_df.empty:
        unique_zk = zk_df.drop_duplicates(subset=['normalized_name'])[['normalized_name', 'personnel id', 'attendance_name']]
        candidates = unique_zk['normalized_name'].tolist()
        
        for idx, row in employee_df.iterrows():
            emp_norm = row['normalized_name']
            if not emp_norm:  # Skip empty names
                continue
            best_cand, ratio = get_best_match(emp_norm, candidates, threshold=85)
            if best_cand:
                match_row = unique_zk[unique_zk['normalized_name'] == best_cand].iloc[0]
                employee_df.at[idx, 'personnel_id'] = match_row['personnel id']
                employee_df.at[idx, 'attendance_name'] = match_row['attendance_name']
                logging.info(f"Fuzzy matched zk for '{row['name']}' to '{match_row['attendance_name']}' (ratio: {ratio:.2f}%)")
            else:
                logging.info(f"No zk match found for '{row['name']}'")
    
    # Fuzzy match with remote for floor and remote_days
    if not remote_df.empty:
        unique_remote = remote_df.drop_duplicates(subset=['normalized_name'])
        candidates = unique_remote['normalized_name'].tolist()
        
        for idx, row in employee_df.iterrows():
            emp_norm = row['normalized_name']
            if not emp_norm:  # Skip empty names
                continue
            best_cand, ratio = get_best_match(emp_norm, candidates, threshold=80)
            if best_cand:
                match_row = unique_remote[unique_remote['normalized_name'] == best_cand].iloc[0]
                employee_df.at[idx, 'remote_name'] = match_row.get('name', pd.NA)
                employee_df.at[idx, 'remote_location'] = match_row.get('location', pd.NA)
                employee_df.at[idx, 'office'] = match_row.get('office', pd.NA)
                for i in range(1, 4):
                    col = f'remote_day_{i}'
                    val = match_row.get(col, pd.NA)
                    if pd.notna(val):
                        employee_df.at[idx, col] = val
                logging.info(f"Fuzzy matched remote for '{row['name']}' to office '{employee_df.at[idx, 'office']}' (ratio: {ratio:.2f}%)")  

            else:
                logging.info(f"No remote match found for '{row['name']}'")
    
    # Drop normalized_name
    employee_df = employee_df.drop(columns=['normalized_name'])
    
    return employee_df

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'DATA')
    employee_file = os.path.join(data_dir, 'employee_data.csv')
    
    logging.info("Starting data processing...")
    
    # Load employee data
    try:
        employee_df = pd.read_csv(employee_file)
        logging.info("Loaded employee_data.csv")
    except Exception as e:
        logging.error(f"Error loading employee_data.csv: {e}")
        return
    
    # Assign department based on employee id
    if 'employee_id' in employee_df.columns:
        employee_df['department'] = employee_df['employee_id'].apply(assign_department)
    else:
        logging.warning("No 'employee id' column found in employee_data.csv")

    
    # Combine remote days
    remote_df = combine_remote_days(data_dir)
    
    # Combine zkaccess
    zk_df = combine_zkaccess(data_dir)
    
    # Enhance
    enhanced_df = enhance_employee_data(employee_df, zk_df, remote_df)
    
    # Output to new file
    output_file = os.path.join(data_dir, 'new_employee_data.csv')
    enhanced_df.to_csv(output_file, index=False)
    logging.info(f"Output saved to {output_file}")

if __name__ == "__main__":
    main()