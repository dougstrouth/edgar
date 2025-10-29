
import duckdb
import os
import sys
from pathlib import Path

# --- BEGIN: Add project root to sys.path ---
# This allows the script to be run from anywhere and still find the utils module
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))
# --- END: Add project root to sys.path ---

try:
    from utils.config_utils import AppConfig
except ImportError as e:
    print(f"FATAL: Could not import utility modules: {e}", file=sys.stderr)
    sys.exit(1)

try:
    config = AppConfig(calling_script_path=Path(__file__))
    db_file = config.DB_FILE_STR
    print(f"Connecting to database: {db_file}")
    con = duckdb.connect(database=db_file, read_only=True)
    
    print("\n--- Analyzing orphaned facts ---")

    # Group by taxonomy
    print("\n--- Top 10 taxonomies for orphaned facts ---")
    taxonomy_counts = con.execute("SELECT taxonomy, COUNT(*) as count FROM xbrl_facts_orphaned GROUP BY taxonomy ORDER BY count DESC LIMIT 10;").fetchall()
    for row in taxonomy_counts:
        print(row)

    # Group by CIK
    print("\n--- Top 10 CIKs for orphaned facts ---")
    cik_counts = con.execute("SELECT cik, COUNT(*) as count FROM xbrl_facts_orphaned GROUP BY cik ORDER BY count DESC LIMIT 10;").fetchall()
    for row in cik_counts:
        print(row)

    # Group by form
    print("\n--- Top 10 forms for orphaned facts ---")
    form_counts = con.execute("SELECT form, COUNT(*) as count FROM xbrl_facts_orphaned GROUP BY form ORDER BY count DESC LIMIT 10;").fetchall()
    for row in form_counts:
        print(row)

    # Group by taxonomy and tag_name
    print("\n--- Top 10 taxonomy/tag_name combinations for orphaned facts ---")
    tag_counts = con.execute("SELECT taxonomy, tag_name, COUNT(*) as count FROM xbrl_facts_orphaned GROUP BY taxonomy, tag_name ORDER BY count DESC LIMIT 10;").fetchall()
    for row in tag_counts:
        print(row)

except Exception as e:
    print(f"An error occurred: {e}", file=sys.stderr)
    sys.exit(1)

