"""Generate normalized unique-value tables and validations for AI use.

This module analyzes existing DuckDB tables (e.g. `xbrl_facts`, `xbrl_tags`,
`updated_ticker_info`) and writes compact tables with distinct values, a
case-normalized form for categorical values, and a flag indicating likely
monetary values. The output table `ai_unique_values` is intentionally generic
so it can be queried by downstream AI workflows.

Usage:
  - Call `create_ai_unique_value_table(con, source_tables=[...])`
  - Or run from CLI: `python -m data_processing.unique_value_tables`
"""
from typing import Any, Dict, Iterable, List, Optional, Tuple
import re
import duckdb


MONEY_RE = re.compile(r"\$|\bUSD\b|\bUS\s?Dollars?\b|€|£|¥|\b(euro|dollar|pound|yen)\b", re.IGNORECASE)
NUMERIC_WITH_COMMAS_RE = re.compile(r"^-?\(?\$?[0-9,]+(?:\.[0-9]+)?\)?$")


def normalize_category(val: Any, column_name: Optional[str] = None) -> Optional[str]:
    """Return a case-normalized string for categorical values, or None.

    - If val is None -> None
    - If `column_name` is 'cik' -> return digits-only, without leading zeros (for regex-friendly matching)
    - If val is string-like -> trimmed and lowercased
    - Otherwise -> None (non-categorical)
    """
    if val is None:
        return None
    # Special handling for CIK to make regex matching easier
    if column_name and column_name.lower() == "cik":
        s = str(val).strip()
        digits = re.sub(r"\D", "", s)
        if digits == "":
            return None
        # remove leading zeros for regex friendliness; keep single '0' if that's the value
        norm = digits.lstrip("0")
        return norm if norm != "" else "0"
    if isinstance(val, str):
        v = val.strip()
        if v == "":
            return None
        return v.lower()
    return None


def looks_like_monetary(val: Any, column_name: Optional[str] = None) -> bool:
    """Heuristic detector for monetary values.

    Accepts an optional `column_name` to avoid false positives on known
    identifier/count/date columns (e.g., `cik`, `fy`, `rn`, `date`).
    """
    if val is None:
        return False
    # Exclude known non-monetary columns by name
    if column_name:
        lower_col = column_name.lower()
        non_money_tokens = ("cik", "fy", "rn", "id", "count", "date", "time", "year", "volume", "round_lot", "total_employees")
        if any(tok in lower_col for tok in non_money_tokens):
            return False

    # Basic type checks
    if isinstance(val, (int, float)):
        # numeric types could be monetary but avoid small integers that are likely counts
        if isinstance(val, int) and abs(val) < 10000:
            return False
        return True

    s = str(val).strip()
    if s == "":
        return False
    # Exclude boolean-like tokens
    if s.lower() in ("true", "false", "yes", "no", "nan", "null", "none"):
        return False

    # Currency words or symbols
    if MONEY_RE.search(s):
        return True
    # Parentheses indicating (1,234)
    if s.startswith("(") and s.endswith(")"):
        inner = s[1:-1].strip()
        if NUMERIC_WITH_COMMAS_RE.match(inner) or inner.replace(',', '').replace('.', '').isdigit():
            return True
    # Numeric-like with optional $ and commas/decimals
    if NUMERIC_WITH_COMMAS_RE.match(s) or s.replace(',', '').replace('.', '').lstrip('-').isdigit():
        # If column name indicates identifier/count, we've already excluded above.
        # For remaining numeric-like strings, consider them monetary only when they
        # contain currency symbol or are large numbers (>=10000)
        plain_digits = re.sub(r"[^0-9.-]", "", s)
        try:
            v = float(plain_digits)
            if abs(v) >= 10000:
                return True
        except Exception:
            pass
    return False


def get_table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> List[Tuple[str, str]]:
    """Return list of (column_name, data_type) for table in DuckDB.

    Uses information_schema.columns so it works without depending on
    DuckDB PRAGMA variations.
    """
    q = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?;"
    rows = con.execute(q, [table_name]).fetchall()
    return [(r[0], r[1]) for r in rows]


def gather_unique_values_for_column(con: duckdb.DuckDBPyConnection, table: str, column: str, limit: int = 10000) -> List[Tuple[Any, int]]:
    """Return list of (value, count) for distinct values in a column, ordered by count desc."""
    safe_col = f'"{column}"'
    q = f"SELECT {safe_col} AS raw_value, COUNT(*) AS cnt FROM {table} GROUP BY 1 ORDER BY cnt DESC LIMIT {limit};"
    return con.execute(q).fetchall()


def create_ai_unique_value_table(con: duckdb.DuckDBPyConnection, source_tables: Optional[Iterable[str]] = None) -> None:
    """Create or replace `ai_unique_values` table from given source tables.

    Columns:
      - source_table
      - column_name
      - raw_value
      - normalized_value
      - is_monetary
      - sample_count
    """
    con.execute("CREATE TABLE IF NOT EXISTS ai_unique_values (source_table VARCHAR, column_name VARCHAR, raw_value VARCHAR, normalized_value VARCHAR, is_monetary BOOLEAN, sample_count BIGINT);")
    # clear existing rows for fresh run
    con.execute("DELETE FROM ai_unique_values;")

    if not source_tables:
        # sensible default targets
        source_tables = ["xbrl_facts", "xbrl_tags", "updated_ticker_info", "tickers"]

    for tbl in source_tables:
        # check table exists
        exists = con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1;", [tbl]).fetchone()
        if not exists:
            continue
        cols = get_table_columns(con, tbl)
        for col_name, data_type in cols:
            try:
                values = gather_unique_values_for_column(con, tbl, col_name, limit=10000)
            except Exception:
                # If SQL fails for some column types, skip
                continue
            to_insert = []
            for raw_val, cnt in values:
                normalized = normalize_category(raw_val, column_name=col_name)
                monetary = looks_like_monetary(raw_val, column_name=col_name)
                # coerce raw_val to string for storage
                raw_s = None if raw_val is None else str(raw_val)
                to_insert.append((tbl, col_name, raw_s, normalized, monetary, int(cnt)))
            # batch insert
            if to_insert:
                con.executemany("INSERT INTO ai_unique_values VALUES (?, ?, ?, ?, ?, ?);", to_insert)
                # Also create/replace a per-source partitioned table for faster, domain-specific queries
                safe_tbl = re.sub(r'[^0-9a-zA-Z]', '_', tbl)
                per_table_name = f"ai_unique_values_{safe_tbl}"
                con.execute(f"CREATE TABLE IF NOT EXISTS \"{per_table_name}\" (source_table VARCHAR, column_name VARCHAR, raw_value VARCHAR, normalized_value VARCHAR, is_monetary BOOLEAN, sample_count BIGINT);")
                # delete existing rows for this run to keep idempotent
                con.execute(f"DELETE FROM \"{per_table_name}\" WHERE source_table = ?;", [tbl])
                con.executemany(f"INSERT INTO \"{per_table_name}\" VALUES (?, ?, ?, ?, ?, ?);", to_insert)


if __name__ == "__main__":
    import os
    # Try to obtain DB_FILE from environment like the project expects
    db_file = os.environ.get("DB_FILE", None)
    if not db_file:
        print("Please set DB_FILE environment variable to your DuckDB file and re-run.")
    else:
        con = duckdb.connect(database=db_file, read_only=False)
        create_ai_unique_value_table(con)
        print("Created/updated table `ai_unique_values`.")
