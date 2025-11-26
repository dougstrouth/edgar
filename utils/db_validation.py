# -*- coding: utf-8 -*-
"""
Database Validation Utilities

Provides comprehensive validation functions for testing database integrity,
including schema validation, referential integrity checks, and data quality tests.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import duckdb

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Results from a validation check."""
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self):
        status = "✓ PASS" if self.passed else "✗ FAIL"
        return f"{status}: {self.message}"


class DatabaseValidator:
    """Validates database structure and data integrity."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """Initialize validator with a database connection."""
        self.conn = conn
        self.results: List[ValidationResult] = []
    
    def reset_results(self):
        """Clear validation results."""
        self.results = []
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of validation results."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": (passed / total * 100) if total > 0 else 0
        }
    
    # Schema Validation
    
    def validate_table_exists(self, table_name: str) -> ValidationResult:
        """Check if a table exists."""
        try:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            ).fetchone()
            
            exists = result[0] > 0 if result else False
            
            if exists:
                res = ValidationResult(True, f"Table '{table_name}' exists")
            else:
                res = ValidationResult(False, f"Table '{table_name}' does not exist")
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error checking table '{table_name}': {e}")
            self.results.append(res)
            return res
    
    def validate_columns(self, table_name: str, expected_columns: List[str]) -> ValidationResult:
        """Validate that a table has the expected columns."""
        try:
            result = self.conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            ).fetchall()
            
            actual_columns = {row[0].lower() for row in result}
            expected_set = {col.lower() for col in expected_columns}
            
            missing = expected_set - actual_columns
            extra = actual_columns - expected_set
            
            if not missing and not extra:
                res = ValidationResult(
                    True,
                    f"Table '{table_name}' has all expected columns",
                    {"column_count": len(actual_columns)}
                )
            else:
                details = {}
                if missing:
                    details["missing"] = list(missing)
                if extra:
                    details["extra"] = list(extra)
                
                res = ValidationResult(
                    False,
                    f"Table '{table_name}' column mismatch",
                    details
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error validating columns for '{table_name}': {e}")
            self.results.append(res)
            return res
    
    def validate_primary_key(self, table_name: str, expected_pk: List[str]) -> ValidationResult:
        """Validate that a table has the expected primary key."""
        try:
            # Get primary key constraints
            result = self.conn.execute(f"""
                SELECT column_name 
                FROM information_schema.key_column_usage 
                WHERE table_name = '{table_name}' 
                AND constraint_name LIKE '%_pkey'
                ORDER BY ordinal_position
            """).fetchall()
            
            actual_pk = [row[0].lower() for row in result]
            expected_pk_lower = [col.lower() for col in expected_pk]
            
            if actual_pk == expected_pk_lower:
                res = ValidationResult(
                    True,
                    f"Table '{table_name}' has correct primary key: {expected_pk}"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Table '{table_name}' primary key mismatch",
                    {"expected": expected_pk, "actual": actual_pk}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error validating primary key for '{table_name}': {e}")
            self.results.append(res)
            return res
    
    # Data Integrity Validation
    
    def validate_no_duplicates(self, table_name: str, key_columns: List[str]) -> ValidationResult:
        """Check for duplicate records based on key columns."""
        try:
            key_cols = ", ".join(key_columns)
            result = self.conn.execute(f"""
                SELECT COUNT(*) as dup_count
                FROM (
                    SELECT {key_cols}, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY {key_cols}
                    HAVING COUNT(*) > 1
                )
            """).fetchone()
            
            dup_count = result[0] if result else 0
            
            if dup_count == 0:
                res = ValidationResult(
                    True,
                    f"No duplicates in '{table_name}' on {key_columns}"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Found {dup_count} duplicate groups in '{table_name}'",
                    {"duplicate_count": dup_count, "key_columns": key_columns}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error checking duplicates in '{table_name}': {e}")
            self.results.append(res)
            return res
    
    def validate_no_nulls(self, table_name: str, column: str) -> ValidationResult:
        """Check that a column has no NULL values."""
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL
            """).fetchone()
            
            null_count = result[0] if result else 0
            
            if null_count == 0:
                res = ValidationResult(
                    True,
                    f"No NULL values in '{table_name}.{column}'"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Found {null_count} NULL values in '{table_name}.{column}'",
                    {"null_count": null_count}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error checking NULLs in '{table_name}.{column}': {e}")
            self.results.append(res)
            return res
    
    # Referential Integrity Validation
    
    def validate_foreign_key(
        self, 
        child_table: str, 
        child_column: str, 
        parent_table: str, 
        parent_column: str,
        allow_null: bool = False
    ) -> ValidationResult:
        """Validate referential integrity between tables."""
        try:
            # Build WHERE clause based on allow_null
            if allow_null:
                # Exclude NULL values from orphan check
                where_clause = f"WHERE c.{child_column} IS NOT NULL AND NOT EXISTS"
            else:
                # Include all records in check (NULLs will be considered orphans)
                where_clause = "WHERE NOT EXISTS"
            
            result = self.conn.execute(f"""
                SELECT COUNT(*) as orphan_count
                FROM {child_table} c
                {where_clause} (
                    SELECT 1 FROM {parent_table} p 
                    WHERE p.{parent_column} = c.{child_column}
                )
            """).fetchone()
            
            orphan_count = result[0] if result else 0
            
            if orphan_count == 0:
                res = ValidationResult(
                    True,
                    f"All {child_table}.{child_column} values exist in {parent_table}.{parent_column}"
                )
            else:
                # Get sample orphaned values (use same WHERE clause logic)
                if allow_null:
                    sample_where = f"WHERE c.{child_column} IS NOT NULL AND NOT EXISTS"
                else:
                    sample_where = "WHERE NOT EXISTS"
                
                sample = self.conn.execute(f"""
                    SELECT DISTINCT c.{child_column}
                    FROM {child_table} c
                    {sample_where} (
                        SELECT 1 FROM {parent_table} p 
                        WHERE p.{parent_column} = c.{child_column}
                    )
                    LIMIT 5
                """).fetchall()
                
                res = ValidationResult(
                    False,
                    f"Found {orphan_count} orphaned records in {child_table}.{child_column}",
                    {
                        "orphan_count": orphan_count,
                        "sample_orphaned_values": [row[0] for row in sample]
                    }
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(
                False, 
                f"Error validating FK {child_table}.{child_column} -> {parent_table}.{parent_column}: {e}"
            )
            self.results.append(res)
            return res
    
    # Data Quality Validation
    
    def validate_value_range(
        self, 
        table_name: str, 
        column: str, 
        min_val: Optional[Any] = None, 
        max_val: Optional[Any] = None
    ) -> ValidationResult:
        """Validate that numeric values fall within expected range."""
        try:
            conditions = []
            if min_val is not None:
                conditions.append(f"{column} < {min_val}")
            if max_val is not None:
                conditions.append(f"{column} > {max_val}")
            
            where_clause = " OR ".join(conditions)
            
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE {column} IS NOT NULL AND ({where_clause})
            """).fetchone()
            
            out_of_range = result[0] if result else 0
            
            if out_of_range == 0:
                range_str = f"[{min_val}, {max_val}]"
                res = ValidationResult(
                    True,
                    f"All {table_name}.{column} values in range {range_str}"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Found {out_of_range} values outside range in {table_name}.{column}",
                    {"out_of_range_count": out_of_range}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error validating range for '{table_name}.{column}': {e}")
            self.results.append(res)
            return res
    
    def validate_row_count(
        self, 
        table_name: str, 
        min_rows: Optional[int] = None, 
        max_rows: Optional[int] = None
    ) -> ValidationResult:
        """Validate that table has expected number of rows."""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result[0] if result else 0
            
            passed = True
            messages = []
            
            if min_rows is not None and row_count < min_rows:
                passed = False
                messages.append(f"below minimum {min_rows}")
            
            if max_rows is not None and row_count > max_rows:
                passed = False
                messages.append(f"above maximum {max_rows}")
            
            if passed:
                res = ValidationResult(
                    True,
                    f"Table '{table_name}' has {row_count} rows (within expected range)"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Table '{table_name}' has {row_count} rows: {', '.join(messages)}",
                    {"row_count": row_count}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(False, f"Error validating row count for '{table_name}': {e}")
            self.results.append(res)
            return res
    
    def validate_date_consistency(
        self,
        table_name: str,
        start_date_col: str,
        end_date_col: str
    ) -> ValidationResult:
        """Validate that end dates are not before start dates."""
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {start_date_col} IS NOT NULL 
                  AND {end_date_col} IS NOT NULL
                  AND {end_date_col} < {start_date_col}
            """).fetchone()
            
            invalid_count = result[0] if result else 0
            
            if invalid_count == 0:
                res = ValidationResult(
                    True,
                    f"All date ranges valid in '{table_name}' ({start_date_col} <= {end_date_col})"
                )
            else:
                res = ValidationResult(
                    False,
                    f"Found {invalid_count} invalid date ranges in '{table_name}'",
                    {"invalid_count": invalid_count}
                )
            
            self.results.append(res)
            return res
            
        except Exception as e:
            res = ValidationResult(
                False, 
                f"Error validating date consistency in '{table_name}': {e}"
            )
            self.results.append(res)
            return res


def validate_edgar_database(conn: duckdb.DuckDBPyConnection) -> DatabaseValidator:
    """
    Run comprehensive validation on the EDGAR database.
    
    Returns:
        DatabaseValidator with all validation results
    """
    validator = DatabaseValidator(conn)
    
    # Schema validation
    logger.info("Validating database schema...")
    
    validator.validate_table_exists("companies")
    validator.validate_table_exists("tickers")
    validator.validate_table_exists("former_names")
    validator.validate_table_exists("filings")
    validator.validate_table_exists("xbrl_tags")
    validator.validate_table_exists("xbrl_facts")
    
    # Column validation
    validator.validate_columns("companies", [
        "cik", "primary_name", "entity_type", "sic", "sic_description"
    ])
    
    validator.validate_columns("filings", [
        "accession_number", "cik", "filing_date", "form"
    ])
    
    # Primary key validation
    logger.info("Validating primary keys...")
    validator.validate_primary_key("companies", ["cik"])
    validator.validate_primary_key("filings", ["accession_number"])
    
    # Data integrity
    logger.info("Validating data integrity...")
    validator.validate_no_duplicates("companies", ["cik"])
    validator.validate_no_duplicates("filings", ["accession_number"])
    validator.validate_no_duplicates("tickers", ["ticker", "exchange"])
    # Ensure ticker symbol itself is unique
    validator.validate_no_duplicates("tickers", ["ticker"])
    
    # Required fields
    validator.validate_no_nulls("companies", "cik")
    validator.validate_no_nulls("filings", "accession_number")
    validator.validate_no_nulls("filings", "cik")
    validator.validate_no_nulls("filings", "form")
    
    # Referential integrity
    logger.info("Validating referential integrity...")
    validator.validate_foreign_key("filings", "cik", "companies", "cik")
    validator.validate_foreign_key("tickers", "cik", "companies", "cik")
    validator.validate_foreign_key("former_names", "cik", "companies", "cik")
    validator.validate_foreign_key("xbrl_facts", "cik", "companies", "cik")
    validator.validate_foreign_key("xbrl_facts", "accession_number", "filings", "accession_number")
    # Tag reference integrity
    validator.validate_foreign_key("xbrl_facts", "tag_id", "xbrl_tags", "tag_id")

    # Consistency: xbrl_facts.cik must match filings.cik for accession_number
    try:
        mismatch_row = conn.execute(
            """
            SELECT COUNT(*) FROM xbrl_facts xf
            JOIN filings f ON f.accession_number = xf.accession_number
            WHERE xf.cik IS NOT NULL AND f.cik IS NOT NULL AND xf.cik <> f.cik
            """
        ).fetchone()
        mismatches = mismatch_row[0] if mismatch_row else 0
        if mismatches == 0:
            validator.results.append(
                ValidationResult(True, "All xbrl_facts.cik values match filings.cik for accession_number")
            )
        else:
            sample = conn.execute(
                """
                SELECT xf.accession_number, xf.cik AS facts_cik, f.cik AS filings_cik
                FROM xbrl_facts xf
                JOIN filings f ON f.accession_number = xf.accession_number
                WHERE xf.cik IS NOT NULL AND f.cik IS NOT NULL AND xf.cik <> f.cik
                LIMIT 5
                """
            ).fetchall()
            validator.results.append(
                ValidationResult(
                    False,
                    f"Found {mismatches} xbrl_facts rows with cik mismatch vs filings",
                    {"mismatch_count": mismatches, "sample": sample},
                )
            )
    except Exception as e:
        validator.results.append(
            ValidationResult(False, f"Error validating xbrl_facts.cik matches filings.cik: {e}")
        )
    
    # Data quality
    logger.info("Validating data quality...")
    validator.validate_row_count("companies", min_rows=1)
    validator.validate_row_count("filings", min_rows=1)

    # Stock history validations (Polygon/Massive gathered data)
    logger.info("Validating stock_history table...")
    exists_row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_history'"
    ).fetchone()
    stock_history_exists = exists_row[0] > 0 if exists_row else False
    validator.validate_table_exists("stock_history")
    if stock_history_exists:
        validator.validate_columns(
            "stock_history",
            [
                "ticker",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
        )
        # No duplicate OHLCV rows per ticker/date
        validator.validate_no_duplicates("stock_history", ["ticker", "date"])
        # Required fields non-null
        validator.validate_no_nulls("stock_history", "ticker")
        validator.validate_no_nulls("stock_history", "date")
        # Price columns should be non-negative; high should be >= low
        validator.validate_value_range("stock_history", "open", min_val=0)
        validator.validate_value_range("stock_history", "high", min_val=0)
        validator.validate_value_range("stock_history", "low", min_val=0)
        validator.validate_value_range("stock_history", "close", min_val=0)
        validator.validate_value_range("stock_history", "volume", min_val=0)

        # Additional logical check: high >= low
        try:
            result = conn.execute(
                """
                SELECT COUNT(*) FROM stock_history
                WHERE high IS NOT NULL AND low IS NOT NULL AND high < low
                """
            ).fetchone()
            invalid_hilo = result[0] if result else 0
            if invalid_hilo == 0:
                validator.results.append(
                    ValidationResult(True, "All stock_history rows satisfy high >= low")
                )
            else:
                validator.results.append(
                    ValidationResult(
                        False,
                        f"Found {invalid_hilo} stock_history rows with high < low",
                        {"invalid_count": invalid_hilo},
                    )
                )
        except Exception as e:
            validator.results.append(
                ValidationResult(False, f"Error validating high>=low in stock_history: {e}")
            )

        # Optional FK-style check: stock_history.ticker exists in tickers.ticker
        try:
            tickers_exists_row = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tickers'"
            ).fetchone()
            tickers_exists = tickers_exists_row[0] > 0 if tickers_exists_row else False
            if tickers_exists:
                # Count orphan tickers present in stock_history but not in tickers
                orphan_row = conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT sh.ticker
                        FROM stock_history sh
                        WHERE NOT EXISTS (
                            SELECT 1 FROM tickers t WHERE t.ticker = sh.ticker
                        )
                    )
                    """
                ).fetchone()
                orphan_count = orphan_row[0] if orphan_row else 0
                if orphan_count == 0:
                    validator.results.append(
                        ValidationResult(True, "All stock_history.ticker values exist in tickers.ticker")
                    )
                else:
                    sample = conn.execute(
                        """
                        SELECT DISTINCT sh.ticker
                        FROM stock_history sh
                        WHERE NOT EXISTS (
                            SELECT 1 FROM tickers t WHERE t.ticker = sh.ticker
                        )
                        LIMIT 5
                        """
                    ).fetchall()
                    validator.results.append(
                        ValidationResult(
                            False,
                            f"Found {orphan_count} stock_history tickers missing from tickers",
                            {"orphan_count": orphan_count, "sample_orphaned_values": [r[0] for r in sample]},
                        )
                    )
        except Exception as e:
            validator.results.append(
                ValidationResult(False, f"Error validating stock_history.ticker -> tickers.ticker: {e}")
            )
    
    return validator
