import os
import sys
import pandas as pd
import argparse
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Add root directory to path to import config.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Add debug prints for path checking
print("Current working directory:", os.getcwd())
print("Script directory:", os.path.dirname(os.path.abspath(__file__)))
print(".env file exists:", os.path.exists('.env'))

# Load environment variables from .env file
# Try with explicit path
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
print("Looking for .env at:", dotenv_path)
print(".env file exists at path:", os.path.exists(dotenv_path))
load_dotenv(dotenv_path)

def clean_oracle_report_data(df):
    """Clean and prepare Oracle Report data for upload"""
    print("\nCleaning Oracle Report data")
    print("Initial DataFrame shape:", df.shape)
    print("Columns:", df.columns.tolist())
    
    # Convert array-like strings to proper string representation
    for column in df.columns:
        if df[column].iloc[0] and isinstance(df[column].iloc[0], str) and df[column].iloc[0].startswith('[') and df[column].iloc[0].endswith(']'):
            print(f"Converting array-like strings in column: {column}")
            df[column] = df[column].apply(lambda x: str(x))
    
    # Convert boolean values to strings
    bool_columns = df.select_dtypes(include=['bool']).columns
    for column in bool_columns:
        print(f"Converting boolean values in column: {column}")
        df[column] = df[column].astype(str).str.lower()
    
    print("Final DataFrame shape:", df.shape)
    return df

def clean_withdrawal_times_data(df):
    """Clean and prepare Withdrawal Times data for upload"""
    print("\nCleaning Withdrawal Times data")
    print("Initial DataFrame shape:", df.shape)
    print("Columns:", df.columns.tolist())
    
    # Convert any None values to empty strings
    df = df.fillna('')
    
    # Ensure all numeric columns are properly formatted
    numeric_columns = ['Amount', 'FinalizationIn (days)', 'Weighted Duration (days)']
    for column in numeric_columns:
        if column in df.columns:
            print(f"Formatting numeric column: {column}")
            # Convert to string while preserving numeric values
            df[column] = df[column].apply(lambda x: str(x) if x != '' else '')
    
    print("Final DataFrame shape:", df.shape)
    return df

def upload_to_dune(df, description, table_name):
    """Upload DataFrame to Dune Analytics as a dataset"""
    print(f"Initializing Dune client for {table_name}...")
    dune = DuneClient.from_env()
    
    # Convert DataFrame to CSV string
    print("Converting DataFrame to CSV...")
    csv_data = df.to_csv(index=False)
    
    # Upload to Dune
    print(f"Uploading data to Dune (table: {table_name})...")
    result = dune.upload_csv(
        data=csv_data,
        description=description,
        table_name=table_name,
        is_private=False
    )
    
    if isinstance(result, bool):
        if result:
            print(f"Successfully uploaded {table_name} data to Dune (upload method returned True).")
        else:
            print(f"Failed to upload {table_name} data to Dune (upload method returned False).")
    elif hasattr(result, "table_name"):
        print(f"Successfully uploaded data to Dune. Table name: {result.table_name}")
    else:
        print("Upload completed, but the result is not in the expected format.")
    
    return result

def upload_oracle_report():
    """Upload the Oracle Report data to Dune"""
    try:
        print("\n=== UPLOADING ORACLE REPORT DATA ===")
        # Read the CSV file
        print(f"Reading Oracle Report CSV file from {config.DEFAULT_ORACLE_REPORT_PATH}...")
        df = pd.read_csv(config.DEFAULT_ORACLE_REPORT_PATH)
        
        # Clean data
        df = clean_oracle_report_data(df)
        
        # Upload to Dune
        result = upload_to_dune(
            df=df,
            description="Lido Oracle Report Data",
            table_name=config.ORACLE_REPORT_TABLE
        )
        
        print("Oracle Report upload completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error occurred during Oracle Report upload: {str(e)}")
        return False

def upload_withdrawal_times():
    """Upload the Withdrawal Times data to Dune"""
    try:
        print("\n=== UPLOADING WITHDRAWAL TIMES DATA ===")
        # Read the CSV file
        print(f"Reading Withdrawal Times CSV file from {config.DEFAULT_WITHDRAWAL_TIMES_PATH}...")
        df = pd.read_csv(config.DEFAULT_WITHDRAWAL_TIMES_PATH)
        
        # Clean data
        df = clean_withdrawal_times_data(df)
        
        # Upload to Dune
        result = upload_to_dune(
            df=df,
            description="Lido Withdrawal Times Analysis",
            table_name=config.WITHDRAWAL_TIMES_TABLE
        )
        
        print("Withdrawal Times upload completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error occurred during Withdrawal Times upload: {str(e)}")
        return False

def main():
    print("Script started successfully from CLI!")
    parser = argparse.ArgumentParser(description='Upload Lido data to Dune Analytics')
    parser.add_argument('--type', choices=['oracle', 'withdrawal', 'both'], 
                        default='both', help='Type of data to upload (default: both)')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Run without actually uploading to Dune')
    args = parser.parse_args()
    
    # Verify DUNE_API_KEY is present and not empty
    dune_api_key = os.getenv("DUNE_API_KEY")
    if not dune_api_key:
        print("Error: DUNE_API_KEY environment variable is missing or empty")
        return 1
    
    print(f"DUNE_API_KEY present with length: {len(dune_api_key)}")
    
    success = True
    
    if args.dry_run:
        print("DRY RUN MODE: Would upload the following data:")
        print(f"Oracle Report: {config.DEFAULT_ORACLE_REPORT_PATH}")
        print(f"Withdrawal Times: {config.DEFAULT_WITHDRAWAL_TIMES_PATH}")
        return 0
    
    # Execute uploads based on the type argument
    if args.type in ['oracle', 'both']:
        if os.path.exists(config.DEFAULT_ORACLE_REPORT_PATH):
            if not upload_oracle_report():
                success = False
        else:
            print(f"Warning: {config.DEFAULT_ORACLE_REPORT_PATH} does not exist, skipping Oracle Report upload")
    
    if args.type in ['withdrawal', 'both']:
        if os.path.exists(config.DEFAULT_WITHDRAWAL_TIMES_PATH):
            if not upload_withdrawal_times():
                success = False
        else:
            print(f"Warning: {config.DEFAULT_WITHDRAWAL_TIMES_PATH} does not exist, skipping Withdrawal Times upload")
    
    return 0 if success else 1

if __name__ == "__main__":
    if os.getenv("DEBUG_MODE") == "1":
        print("Running in debug mode")
        # Add more debug info here
    sys.exit(main()) 