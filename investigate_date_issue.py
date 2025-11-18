import duckdb
import pandas as pd
from pathlib import Path
from utils.logging_utils import setup_logging
from utils.config_utils import AppConfig

# Set up logging and configuration
script_name = Path(__file__).stem
log_dir = Path(__file__).resolve().parent / "logs"
log = setup_logging(script_name, log_dir)
config = AppConfig(calling_script_path=Path(__file__))
DB_FILE = config.DB_FILE


def investigate_date_discrepancies(db_file: str):
    """
    Investigates discrepancies where the period_end_date is significantly different from the filed_date.
    """
    with duckdb.connect(database=db_file, read_only=True) as con:
        query = """
        SELECT 
            cik, 
            accession_number, 
            period_end_date, 
            filed_date
        FROM xbrl_facts
        WHERE period_end_date > filed_date + INTERVAL '1 day'
        ORDER BY filed_date
        LIMIT 10;
        """
        df = con.execute(query).fetchdf()
        print("Date discrepancies:")
        print(df)


def investigate_duplicate_rows(db_file: str):
    """
    Investigates duplicate rows in the xbrl_facts table.
    """
    with duckdb.connect(database=db_file, read_only=True) as con:
        query = """
        SELECT 
            cik, 
            accession_number, 
            period_end_date, 
            filed_date,
            COUNT(*) as count
        FROM xbrl_facts
        GROUP BY 
            cik, 
            accession_number, 
            period_end_date, 
            filed_date
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10;
        """
        df = con.execute(query).fetchdf()
        print("\nDuplicate rows in xbrl_facts table:")
        print(df)


if __name__ == "__main__":
    investigate_date_discrepancies(DB_FILE)
    investigate_duplicate_rows(DB_FILE)