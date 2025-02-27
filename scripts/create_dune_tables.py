#!/usr/bin/env python3

import os
import sys
import argparse
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Add root directory to path to import config.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

def delete_withdrawal_times_table():
    """Delete the Withdrawal Times table in Dune Analytics"""
    load_dotenv()
    dune = DuneClient.from_env()
    
    try:
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
        return False

def delete_oracle_report_table():
    """Delete the Oracle Report table in Dune Analytics"""
    load_dotenv()
    dune = DuneClient.from_env()
    
    try:
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
        return False

def create_withdrawal_times_table():
    """Create the Withdrawal Times table in Dune Analytics"""
    load_dotenv()
    dune = DuneClient.from_env()
    
    # Define schema for withdrawal times table
    schema = [
        {"name": "timestamp", "type": "timestamp"},
        {"name": "amount", "type": "double"},
        {"name": "finalizationin_days", "type": "double"},
        {"name": "weighted_duration_days", "type": "double"}
    ]
    
    try:
        # Create the table
        table_name = config.WITHDRAWAL_TIMES_TABLE
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
        print(f"Error creating withdrawal times table: {e}")
        return False

def create_oracle_report_table():
    """Create the Oracle Report table in Dune Analytics"""
    load_dotenv()
    dune = DuneClient.from_env()
    
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
    
    try:
        # Create the table
        table_name = config.ORACLE_REPORT_TABLE
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
        print(f"Error creating oracle report table: {e}")
        return False

def recreate_withdrawal_times_table():
    """Delete and recreate the Withdrawal Times table"""
    print("Recreating withdrawal times table...")
    delete_withdrawal_times_table()
    return create_withdrawal_times_table()

def recreate_oracle_report_table():
    """Delete and recreate the Oracle Report table"""
    print("Recreating oracle report table...")
    delete_oracle_report_table()
    return create_oracle_report_table()

def main():
    parser = argparse.ArgumentParser(description='Create Dune Analytics tables for Lido data')
    parser.add_argument('--type', choices=['oracle', 'withdrawal', 'both'], 
                        default='both', help='Type of table to create (default: both)')
    parser.add_argument('--action', choices=['create', 'delete', 'recreate'],
                        default='create', help='Action to perform on the table(s) (default: create)')
    args = parser.parse_args()
    
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