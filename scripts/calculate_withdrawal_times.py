import requests
import csv
import numpy as np
import time
import os
import logging
import argparse
import sys
from datetime import datetime
from typing import Tuple, List, Optional
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Add root directory to path to import config.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('withdrawal_times')

def setup_requests_session() -> requests.Session:
    """Configure requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def fetch_withdrawal_time(session: requests.Session, amount: int) -> Tuple[Optional[float], Optional[str]]:
    """Fetch withdrawal time for a specific amount"""
    try:
        response = session.get(
            f'{config.WITHDRAWAL_API_BASE_URL}?amount={amount}',
            headers={'accept': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        
        response_data = response.json()
        finalization_in_ms = response_data['requestInfo'].get('finalizationIn', None)
        finalization_in_days = finalization_in_ms / (1000 * 60 * 60 * 24) if finalization_in_ms is not None else None
        
        return finalization_in_days, None
    except requests.exceptions.RequestException as e:
        return None, f"Request error: {str(e)}"
    except (ValueError, KeyError) as e:
        return None, f"Data parsing error: {str(e)}"

def calculate_weighted_durations(amounts: np.ndarray) -> List[Tuple]:
    """Calculate withdrawal times and weighted durations for all amounts"""
    timestamp = int(time.time())
    results = []
    
    session = setup_requests_session()
    
    # Prepare cumulative calculations
    cumulative_amount = 0
    weighted_sum = 0
    
    for amount in amounts:
        finalization_in_days, error = fetch_withdrawal_time(session, int(amount))
        
        # Calculate incremental amount
        incremental_amount = amount - cumulative_amount
        cumulative_amount += incremental_amount
        
        # Update weighted sum and calculate weighted duration
        if finalization_in_days is not None:
            weighted_sum += incremental_amount * finalization_in_days
            weighted_duration = weighted_sum / cumulative_amount
        else:
            weighted_duration = None
        
        # Store result
        results.append((timestamp, int(amount), finalization_in_days, weighted_duration))
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    return results

def save_results(results: List[Tuple], file_path: str) -> None:
    """Save results to CSV file"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow(['Timestamp', 'Amount', 'FinalizationIn (days)', 'Weighted Duration (days)'])
        # Write all results
        writer.writerows(results)

def main():
    parser = argparse.ArgumentParser(description='Calculate Lido withdrawal finalization times')
    parser.add_argument('--min-amount', type=float, default=config.DEFAULT_MIN_AMOUNT, help='Minimum ETH amount')
    parser.add_argument('--max-amount', type=float, default=config.DEFAULT_MAX_AMOUNT, help='Maximum ETH amount')
    parser.add_argument('--num-points', type=int, default=config.DEFAULT_NUM_POINTS, help='Number of data points')
    parser.add_argument('--output', type=str, default=config.DEFAULT_WITHDRAWAL_TIMES_PATH, help='Output CSV file path')
    args = parser.parse_args()
    
    logger.info("Starting withdrawal time analysis")
    
    # Define the range of amounts using logarithmic scale
    amounts = np.logspace(
        np.log10(args.min_amount),
        np.log10(args.max_amount),
        num=args.num_points,
        base=10
    )
    logger.info(f"Calculating for {len(amounts)} amount points from {args.min_amount} to {args.max_amount} ETH")
    
    # Calculate all withdrawal times
    results = calculate_weighted_durations(amounts)
    
    # Save results to CSV
    save_results(results, args.output)
    
    timestamp = results[0][0] if results else int(time.time())
    logger.info(f"Withdrawal time analysis completed at {datetime.fromtimestamp(timestamp).isoformat()}")
    logger.info(f"Results saved to {args.output}")

if __name__ == "__main__":
    main() 