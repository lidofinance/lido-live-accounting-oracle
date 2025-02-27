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

def process_array_field(value):
    """Process array-like string fields to ensure proper format"""
    # Handle both string representations and actual arrays
    if isinstance(value, str):
        # If it's already a string representation of an array, return as is
        if value.startswith('[') and value.endswith(']'):
            return value
        else:
            # Wrap single value in array brackets
            return f"[{value}]"
    # Convert actual arrays to string representation
    elif isinstance(value, (list, tuple)):
        return str(value)
    # Return all other values as is
    return str(value)

def clean_oracle_report_data(df):
    """Clean and prepare the Oracle Report data"""
    print("\nCleaning Oracle Report data")
    print(f"Initial DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # First, rename columns to match the schema in the Dune table
    column_mapping = {
        'Process Timestamp': 'process_timestamp',
        'Block Number': 'block_number',
        'Block Timestamp': 'block_timestamp',
        'Block Hash': 'block_hash',
        'Consensus Version': 'consensus_version',
        'Reference Slot': 'reference_slot',
        'CL Balance (Gwei)': 'cl_balance_gwei',
        'Number of Validators': 'number_of_validators',
        'Withdrawal Vault Balance (ETH)': 'withdrawal_vault_balance_eth',
        'EL Rewards Vault Balance (ETH)': 'el_rewards_vault_balance_eth',
        'Shares Requested to Burn': 'shares_requested_to_burn',
        'Withdrawal Finalization Batches': 'withdrawal_finalization_batches',
        'Is Bunker Mode': 'is_bunker_mode',
        'Extra Data Format': 'extra_data_format',
        'Extra Data Hash': 'extra_data_hash',
        'Extra Data Items Count': 'extra_data_items_count',
        'Staking Module IDs with Newly Exited Validators': 'staking_module_ids_with_newly_exited_validators',
        'Number of Exited Validators by Staking Module': 'number_of_exited_validators_by_staking_module'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Process array-like columns (convert string representations to JSON-compatible strings)
    array_columns = [
        'withdrawal_finalization_batches',
        'staking_module_ids_with_newly_exited_validators',
        'number_of_exited_validators_by_staking_module'
    ]
    
    for col in array_columns:
        if col in df.columns:
            print(f"Converting array-like strings in column: {column_mapping.get(col, col)}")
            df[col] = df[col].apply(lambda x: process_array_field(x) if pd.notnull(x) else x)
    
    # Process boolean columns
    boolean_columns = ['is_bunker_mode']
    for col in boolean_columns:
        if col in df.columns:
            print(f"Converting boolean values in column: {column_mapping.get(col, col)}")
            df[col] = df[col].apply(lambda x: str(x).lower() == 'true' if pd.notnull(x) else None)
    
    print(f"Final DataFrame shape: {df.shape}")
    return df

def clean_withdrawal_times_data(df):
    """Clean and prepare Withdrawal Times data for upload"""
    print("\nCleaning Withdrawal Times data")
    print(f"Initial DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # First, rename columns to match the schema in the Dune table
    column_mapping = {
        'Timestamp': 'timestamp',
        'Amount': 'amount',
        'FinalizationIn (days)': 'finalization_in_days',
        'Weighted Duration (days)': 'weighted_duration_days'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Convert any None values to empty strings
    df = df.fillna('')
    
    # Ensure all numeric columns are properly formatted
    numeric_columns = ['amount', 'finalization_in_days', 'weighted_duration_days']
    for column in numeric_columns:
        if column in df.columns:
            print(f"Formatting numeric column: {column}")
            # Convert to string while preserving numeric values
            df[column] = df[column].apply(lambda x: str(x) if x != '' else '')
    
    print(f"Final DataFrame shape: {df.shape}")
    return df

def upload_to_dune(df, description, table_name):
    """Upload DataFrame to Dune Analytics by inserting into an existing table"""
    print(f"Initializing Dune client for {table_name}...")
    dune = DuneClient.from_env()
    
    # Convert DataFrame to CSV string
    print("Converting DataFrame to CSV...")
    csv_path = f"temp_{table_name}.csv"
    df.to_csv(csv_path, index=False)
    
    # Get namespace from config
    namespace = config.DUNE_NAMESPACE
    
    # Upload to Dune
    print(f"Inserting data into Dune table: {namespace}.{table_name}...")
    try:
        with open(csv_path, "rb") as data:
            result = dune.insert_table(
                namespace=namespace,
                table_name=table_name,
                data=data,
                content_type="text/csv"
            )
        
        print(f"Successfully inserted data into {namespace}.{table_name}")
        
        # Clean up temp file
        if os.path.exists(csv_path):
            os.remove(csv_path)
            
        return True
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        return False

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