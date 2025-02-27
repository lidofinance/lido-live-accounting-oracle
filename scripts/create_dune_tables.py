#!/usr/bin/env python3

import os
import sys
import argparse
import time
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Add root directory to path to import config.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# More robust .env loading
def load_environment():
    """Load environment variables more reliably"""
    # Try loading from current directory
    load_dotenv()
    
    # Also try explicit path as fallback
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(dotenv_path)
    
    # Debug information
    print("Current working directory:", os.getcwd())
    print("Script directory:", os.path.dirname(os.path.abspath(__file__)))
    print(".env file exists:", os.path.exists('.env'))
    print(".env file exists at path:", os.path.exists(dotenv_path))
    
    # Check if the API key is available
    api_key = os.getenv("DUNE_API_KEY")
    if api_key:
        print(f"DUNE_API_KEY loaded successfully with length: {len(api_key)}")
        return True
    else:
        print("WARNING: DUNE_API_KEY environment variable is missing or empty")
        return False

def get_dune_client():
    """Get Dune client with direct API key handling"""
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise ValueError("DUNE_API_KEY environment variable is missing or empty")
    
    return DuneClient(api_key)

def delete_withdrawal_times_table():
    """Delete the Withdrawal Times table in Dune Analytics"""
    try:
        dune = get_dune_client()
        
        # Delete the table
        table_name = config.WITHDRAWAL_TIMES_TABLE
        result = dune.delete_table(
            namespace=config.DUNE_NAMESPACE,
            table_name=table_name
        )
        print(f"Withdrawal times table deletion result: {result}")
        return True
    except Exception as e:
        print(f"Error deleting withdrawal times table: {e}")
        # Return True even if there's an error - table might not exist yet
        error_str = str(e).lower()
        if "table does not exist" in error_str or "not found" in error_str:
            print("Table doesn't exist, continuing with creation...")
            return True
        return False

def delete_oracle_report_table():
    """Delete the Oracle Report table in Dune Analytics"""
    try:
        dune = get_dune_client()
        
        # Delete the table
        table_name = config.ORACLE_REPORT_TABLE
        result = dune.delete_table(
            namespace=config.DUNE_NAMESPACE,
            table_name=table_name
        )
        print(f"Oracle report table deletion result: {result}")
        return True
    except Exception as e:
        print(f"Error deleting oracle report table: {e}")
        # Return True even if there's an error - table might not exist yet
        error_str = str(e).lower()
        if "table does not exist" in error_str or "not found" in error_str:
            print("Table doesn't exist, continuing with creation...")
            return True
        return False

def create_withdrawal_times_table(retry=True):
    """Create the Withdrawal Times table in Dune Analytics"""
    try:
        dune = get_dune_client()
        
        # Define schema for withdrawal times table
        schema = [
            {"name": "timestamp", "type": "timestamp"},
            {"name": "amount", "type": "double"},
            {"name": "finalizationin_days", "type": "double"},
            {"name": "weighted_duration_days", "type": "double"}
        ]
        
        # Create the table
        table_name = config.WITHDRAWAL_TIMES_TABLE
        try:
            result = dune.create_table(
                namespace=config.DUNE_NAMESPACE,
                table_name=table_name,
                description="Historical stETH withdrawal times by amount",
                schema=schema,
                is_private=False
            )
            print(f"Withdrawal times table creation result: {result}")
            return True
        except Exception as e:
            error_str = str(e)
            # Check if this is the "can't build result" but actually successful case
            if "Table created successfully" in error_str and "KeyError" in error_str:
                print("Table was actually created successfully despite error in response parsing")
                return True
            if "This table already exists" in error_str and retry:
                print("Table still exists after deletion, attempting forced recreation...")
                # Force delete and retry with backoff
                delete_withdrawal_times_table()
                print("Waiting 5 seconds for deletion to propagate...")
                time.sleep(5)  # Wait for deletion to propagate
                # Retry with different table name
                new_table_name = f"{table_name}_{int(time.time())}"
                print(f"Creating table with alternative name: {new_table_name}")
                
                # Update the config with the new name
                config.WITHDRAWAL_TIMES_TABLE = new_table_name
                
                # Retry without allowing further retries to prevent infinite loops
                return create_withdrawal_times_table(retry=False)
            else:
                raise
    except Exception as e:
        print(f"Error creating withdrawal times table: {e}")
        return False

def create_oracle_report_table(retry=True):
    """Create the Oracle Report table in Dune Analytics"""
    try:
        dune = get_dune_client()
        
        # Define schema for oracle report table based on the structure in lido-report.ts
        schema = [
            {"name": "process_timestamp", "type": "integer"},
            {"name": "block_number", "type": "integer"},
            {"name": "block_timestamp", "type": "integer"},
            {"name": "block_hash", "type": "string"},
            {"name": "consensus_version", "type": "integer"},
            {"name": "reference_slot", "type": "integer"},
            {"name": "cl_balance_gwei", "type": "string"},
            {"name": "number_of_validators", "type": "integer"},
            {"name": "withdrawal_vault_balance_eth", "type": "double"},
            {"name": "el_rewards_vault_balance_eth", "type": "double"},
            {"name": "shares_requested_to_burn", "type": "string"},
            {"name": "withdrawal_finalization_batches", "type": "string"},
            {"name": "is_bunker_mode", "type": "boolean"},
            {"name": "extra_data_format", "type": "integer"},
            {"name": "extra_data_hash", "type": "string"},
            {"name": "extra_data_items_count", "type": "integer"},
            {"name": "staking_module_ids_with_newly_exited_validators", "type": "string"},
            {"name": "number_of_exited_validators_by_staking_module", "type": "string"}
        ]
        
        # Create the table
        table_name = config.ORACLE_REPORT_TABLE
        try:
            result = dune.create_table(
                namespace=config.DUNE_NAMESPACE,
                table_name=table_name,
                description="Lido Oracle Report Data",
                schema=schema,
                is_private=False
            )
            print(f"Oracle report table creation result: {result}")
            return True
        except Exception as e:
            if "This table already exists" in str(e) and retry:
                print("Table still exists after deletion, attempting forced recreation...")
                # Force delete and retry with backoff
                delete_oracle_report_table()
                print("Waiting 5 seconds for deletion to propagate...")
                time.sleep(5)  # Wait for deletion to propagate
                # Retry with different table name
                new_table_name = f"{table_name}_{int(time.time())}"
                print(f"Creating table with alternative name: {new_table_name}")
                
                # Update the config with the new name
                config.ORACLE_REPORT_TABLE = new_table_name
                
                # Retry without allowing further retries to prevent infinite loops
                return create_oracle_report_table(retry=False)
            else:
                raise
    except Exception as e:
        print(f"Error creating oracle report table: {e}")
        return False

def recreate_withdrawal_times_table():
    """Delete and recreate the Withdrawal Times table"""
    print("Recreating withdrawal times table...")
    if delete_withdrawal_times_table():
        print("Waiting 3 seconds for deletion to propagate...")
        time.sleep(3)  # Add a delay to allow deletion to propagate
        return create_withdrawal_times_table()
    return False

def recreate_oracle_report_table():
    """Delete and recreate the Oracle Report table"""
    print("Recreating oracle report table...")
    if delete_oracle_report_table():
        print("Waiting 3 seconds for deletion to propagate...")
        time.sleep(3)  # Add a delay to allow deletion to propagate
        return create_oracle_report_table()
    return False

def main():
    parser = argparse.ArgumentParser(description='Create Dune Analytics tables for Lido data')
    parser.add_argument('--type', choices=['oracle', 'withdrawal', 'both'], 
                        default='both', help='Type of table to create (default: both)')
    parser.add_argument('--action', choices=['create', 'delete', 'recreate'],
                        default='create', help='Action to perform on the table(s) (default: create)')
    args = parser.parse_args()
    
    # Load environment variables
    load_environment()
    
    # Verify DUNE_API_KEY is present and not empty
    dune_api_key = os.getenv("DUNE_API_KEY")
    if not dune_api_key:
        print("Error: DUNE_API_KEY environment variable is missing or empty")
        return 1
    
    success = True
    
    # Execute actions based on the type and action arguments
    if args.type in ['withdrawal', 'both']:
        if args.action == 'create':
            if not create_withdrawal_times_table():
                success = False
        elif args.action == 'delete':
            if not delete_withdrawal_times_table():
                success = False
        elif args.action == 'recreate':
            if not recreate_withdrawal_times_table():
                success = False
    
    if args.type in ['oracle', 'both']:
        if args.action == 'create':
            if not create_oracle_report_table():
                success = False
        elif args.action == 'delete':
            if not delete_oracle_report_table():
                success = False
        elif args.action == 'recreate':
            if not recreate_oracle_report_table():
                success = False
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 