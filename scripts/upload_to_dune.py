import os
import pandas as pd
from dune_client.client import DuneClient
from dotenv import load_dotenv

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

def clean_data(df):
    """Clean and prepare the data for upload"""
    print("\nInitial DataFrame shape:", df.shape)
    print("Columns:", df.columns.tolist())
    
    # Convert array-like strings to proper string representation
    for column in df.columns:
        if isinstance(df[column].iloc[0], str) and df[column].iloc[0].startswith('[') and df[column].iloc[0].endswith(']'):
            print(f"Converting array-like strings in column: {column}")
            df[column] = df[column].apply(lambda x: str(x))
    
    # Convert boolean values to strings
    bool_columns = df.select_dtypes(include=['bool']).columns
    for column in bool_columns:
        print(f"Converting boolean values in column: {column}")
        df[column] = df[column].astype(str).str.lower()
    
    print("Final DataFrame shape:", df.shape)
    return df

def upload_to_dune(df):
    """Upload DataFrame to Dune Analytics as a dataset"""
    print("Initializing Dune client...")
    dune = DuneClient.from_env()
    
    # Convert DataFrame to CSV string
    print("Converting DataFrame to CSV...")
    csv_data = df.to_csv(index=False)
    
    # Upload to Dune
    print("Uploading data to Dune...")
    result = dune.upload_csv(
        data=csv_data,
        description="Lido Oracle Report Data",
        table_name="lido_oracle_report_data",
        is_private=False
    )
    
    if isinstance(result, bool):
        if result:
            print("Successfully uploaded data to Dune (upload method returned True).")
        else:
            print("Failed to upload data to Dune (upload method returned False).")
    elif hasattr(result, "table_name"):
        print(f"Successfully uploaded data to Dune. Table name: {result.table_name}")
    else:
        print("Upload completed, but the result is not in the expected format.")
    
    return result

def main():
    try:
        # Verify DUNE_API_KEY is present and not empty
        dune_api_key = os.getenv("DUNE_API_KEY")
        if not dune_api_key:
            raise ValueError("DUNE_API_KEY environment variable is missing or empty")
        
        # Optional: Print to verify the DUNE_API_KEY is loaded (only show length for security)
        print(f"DUNE_API_KEY present with length: {len(dune_api_key)}")
        
        # Read the CSV file
        print("Reading CSV file...")
        df = pd.read_csv("report.csv")
        
        # Clean data
        print("Cleaning data...")
        df = clean_data(df)
        
        # Upload to Dune
        print("Uploading data to Dune...")
        result = upload_to_dune(df)
        
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    main() 