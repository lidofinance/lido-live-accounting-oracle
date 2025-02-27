"""Configuration settings for Lido Analytics."""

# API and data collection settings
WITHDRAWAL_API_BASE_URL = 'https://wq-api.lido.fi/v2/request-time/calculate'
DEFAULT_MIN_AMOUNT = 10
DEFAULT_MAX_AMOUNT = 1000000
DEFAULT_NUM_POINTS = 40

# Dune Analytics settings
ORACLE_REPORT_TABLE = 'lido_oracle_report_data'
WITHDRAWAL_TIMES_TABLE = 'historical_steth_withdrawal_times_by_amount'

# File paths
DEFAULT_WITHDRAWAL_TIMES_PATH = 'src/withdrawal_times.csv'
DEFAULT_ORACLE_REPORT_PATH = 'report.csv'

# Dune namespace (owner of the dataset)
DUNE_NAMESPACE = "asot"