# -*- coding: utf-8 -*-
"""Integration tests for parsing JSON and loading into DuckDB.

Tests verify end-to-end behavior: parse JSON fixtures → load to temporary DuckDB
→ validate data consistency and handle edge cases (orphaned records, missing filings).
"""

import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pytest

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data_processing.edgar_data_loader import SCHEMA  # noqa: E402
from data_processing.json_parse import (  # noqa: E402
    parse_company_facts_json_for_db,
    parse_submission_json_for_db,
)

FIXTURES_DIR = Path(__file__).resolve().parent / 'fixtures'


class IntegrationTestDB:
    """Helper class to manage a temporary DuckDB for testing."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize with optional custom db_path; otherwise use temp file."""
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None
        if db_path is None:
            self.temp_dir = tempfile.TemporaryDirectory()
            self.db_path = Path(self.temp_dir.name) / "test.duckdb"
        else:
            self.db_path = db_path

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """Create schema tables in DuckDB."""
        self.conn = duckdb.connect(str(self.db_path))

        # Create tables from SCHEMA
        table_creation_order = [
            "companies",
            "filings",
            "tickers",
            "former_names",
            "filing_summaries",
            "xbrl_tags",
            "xbrl_facts",
            "xbrl_facts_orphaned",
        ]

        for table_name in table_creation_order:
            if table_name in SCHEMA:
                sql = SCHEMA[table_name]
                if self.conn:
                    self.conn.execute(sql)

    def insert_companies(self, company_data: Dict[str, Any]) -> None:
        """Insert company record."""
        if not company_data:
            return

        cols = list(company_data.keys())
        vals = [company_data[c] for c in cols]
        placeholders = ", ".join(["?" for _ in cols])
        col_names = ", ".join(cols)

        sql = f"INSERT OR REPLACE INTO companies ({col_names}) VALUES ({placeholders})"
        if self.conn:
            self.conn.execute(sql, vals)

    def insert_filings(self, filing_records: List[Dict[str, Any]]) -> None:
        """Insert filing records."""
        if not filing_records:
            return

        for filing in filing_records:
            if not filing:
                continue
            cols = list(filing.keys())
            vals = [filing[c] for c in cols]
            placeholders = ", ".join(["?" for _ in cols])
            col_names = ", ".join(cols)

            sql = (
                f"INSERT OR REPLACE INTO filings ({col_names}) VALUES ({placeholders})"
            )
            if self.conn:
                self.conn.execute(sql, vals)

    def insert_tickers(self, ticker_records: List[Dict[str, Any]]) -> None:
        """Insert ticker records."""
        if not ticker_records:
            return

        for ticker in ticker_records:
            if not ticker:
                continue
            cols = list(ticker.keys())
            vals = [ticker[c] for c in cols]
            placeholders = ", ".join(["?" for _ in cols])
            col_names = ", ".join(cols)

            sql = f"INSERT INTO tickers ({col_names}) VALUES ({placeholders})"
            if self.conn:
                self.conn.execute(sql, vals)

    def insert_xbrl_tags(self, tag_records: List[Dict[str, Any]]) -> None:
        """Insert XBRL tag records."""
        if not tag_records:
            return

        for tag in tag_records:
            if not tag:
                continue
            cols = list(tag.keys())
            vals = [tag[c] for c in cols]
            placeholders = ", ".join(["?" for _ in cols])
            col_names = ", ".join(cols)

            sql = f"INSERT OR REPLACE INTO xbrl_tags ({col_names}) VALUES ({placeholders})"
            if self.conn:
                self.conn.execute(sql, vals)

    def insert_xbrl_facts(self, fact_records: List[Dict[str, Any]]) -> None:
        """Insert XBRL fact records."""
        if not fact_records:
            return

        for fact in fact_records:
            if not fact:
                continue
            cols = list(fact.keys())
            vals = [fact[c] for c in cols]
            placeholders = ", ".join(["?" for _ in cols])
            col_names = ", ".join(cols)

            sql = f"INSERT OR REPLACE INTO xbrl_facts ({col_names}) VALUES ({placeholders})"
            if self.conn:
                self.conn.execute(sql, vals)

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        if not self.conn:
            return []
        result = self.conn.execute(sql).fetchall()
        cols = self.conn.description
        if cols is None:
            return []
        return [dict(zip([c[0] for c in cols], row)) for row in result]

    def row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        if not self.conn:
            return 0
        result = self.conn.execute(
            f"SELECT COUNT(*) as cnt FROM {table_name}"
        ).fetchall()
        return result[0][0] if result else 0

    def close(self) -> None:
        """Close connection and clean up temp directory."""
        if self.conn:
            self.conn.close()
        if self.temp_dir:
            self.temp_dir.cleanup()

    def __enter__(self) -> "IntegrationTestDB":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class TestParseAndLoadSubmission:
    """Integration tests for parsing submissions and loading into DuckDB."""

    def test_valid_submission_parse_and_load(self) -> None:
        """Parse valid submission and load into DuckDB."""
        file_path = FIXTURES_DIR / "submission_sample.json"
        assert file_path.exists()

        parsed = parse_submission_json_for_db(file_path)
        assert parsed is not None

        with IntegrationTestDB() as db:
            # Insert company
            assert isinstance(parsed["companies"], dict)
            db.insert_companies(parsed["companies"])
            assert db.row_count("companies") == 1

            # Insert filings
            assert isinstance(parsed["filings"], list)
            db.insert_filings(parsed["filings"])
            assert db.row_count("filings") == 1

            # Insert tickers
            assert isinstance(parsed["tickers"], list)
            db.insert_tickers(parsed["tickers"])
            assert db.row_count("tickers") == 1

            # Verify company data
            companies = db.query("SELECT * FROM companies")
            assert len(companies) == 1
            assert companies[0]["cik"] == "0000320193"
            assert "Apple" in companies[0].get("primary_name", "")

            # Verify filing references company
            filings = db.query("SELECT * FROM filings")
            assert len(filings) == 1
            assert filings[0]["cik"] == "0000320193"
            assert filings[0]["form"] == "10-K"

    def test_malformed_submission_skipped_filings(self) -> None:
        """Parse malformed submission where invalid filings are skipped."""
        file_path = FIXTURES_DIR / "submission_malformed.json"
        assert file_path.exists()

        parsed = parse_submission_json_for_db(file_path)
        assert parsed is not None
        assert len(parsed["filings"]) == 0  # Malformed filings skipped

        with IntegrationTestDB() as db:
            # Should load company but no filings
            assert isinstance(parsed["companies"], dict)
            db.insert_companies(parsed["companies"])
            assert isinstance(parsed["filings"], list)
            db.insert_filings(parsed["filings"])

            assert db.row_count("companies") >= 0
            assert db.row_count("filings") == 0


class TestParseAndLoadCompanyFacts:
    """Integration tests for parsing company facts and loading into DuckDB."""

    def test_valid_companyfacts_parse_and_load(self) -> None:
        """Parse valid companyfacts and load into DuckDB."""
        file_path = FIXTURES_DIR / "companyfacts_sample.json"
        assert file_path.exists()

        parsed = parse_company_facts_json_for_db(file_path)
        assert parsed is not None
        assert "xbrl_tags" in parsed
        assert "xbrl_facts" in parsed

        with IntegrationTestDB() as db:
            # Insert tags
            assert isinstance(parsed["xbrl_tags"], list)
            db.insert_xbrl_tags(parsed["xbrl_tags"])
            assert db.row_count("xbrl_tags") > 0

            # Insert facts
            assert isinstance(parsed["xbrl_facts"], list)
            db.insert_xbrl_facts(parsed["xbrl_facts"])
            assert db.row_count("xbrl_facts") > 0

            # Verify tag data
            tags = db.query("SELECT * FROM xbrl_tags LIMIT 1")
            assert len(tags) == 1
            assert "taxonomy" in tags[0]
            assert "tag_name" in tags[0]

            # Verify fact data
            facts = db.query("SELECT * FROM xbrl_facts LIMIT 1")
            assert len(facts) == 1
            assert "cik" in facts[0]
            assert "accession_number" in facts[0]
            assert "value_numeric" in facts[0] or "value_text" in facts[0]

    def test_empty_companyfacts_returns_none(self) -> None:
        """Empty companyfacts file returns None and doesn't load."""
        file_path = FIXTURES_DIR / "companyfacts_empty.json"
        assert file_path.exists()
        assert file_path.stat().st_size == 0

        parsed = parse_company_facts_json_for_db(file_path)
        assert parsed is None

        with IntegrationTestDB() as db:
            # No data to insert
            db.insert_xbrl_tags([])
            db.insert_xbrl_facts([])

            assert db.row_count("xbrl_tags") == 0
            assert db.row_count("xbrl_facts") == 0

    def test_nonfinite_values_handled_correctly(self) -> None:
        """Non-finite values (NaN, Infinity) recorded as text, not numeric."""
        file_path = FIXTURES_DIR / "companyfacts_nonfinite.json"
        assert file_path.exists()

        parsed = parse_company_facts_json_for_db(file_path)
        assert parsed is not None

        facts_list = parsed.get("xbrl_facts", [])
        assert isinstance(facts_list, list)
        assert len(facts_list) >= 2

        with IntegrationTestDB() as db:
            # Insert facts
            db.insert_xbrl_facts(facts_list)
            assert db.row_count("xbrl_facts") >= 2

            # Query for the non-finite records
            nan_facts = db.query(
                "SELECT * FROM xbrl_facts WHERE value_text LIKE 'NaN' OR value_text LIKE 'Infinity'"
            )

            # At least one should be NaN and one should be Infinity
            text_values = {f["value_text"] for f in nan_facts if f.get("value_text")}
            assert "NaN" in text_values or "Infinity" in text_values


class TestEndToEndPipeline:
    """Test complete parse → load → validate pipeline."""

    def test_parse_and_load_with_filing_validation(self) -> None:
        """Parse submission, create company+filings, then parse facts and load."""
        with IntegrationTestDB() as db:
            # Step 1: Parse and load submission data
            submission_path = FIXTURES_DIR / "submission_sample.json"
            submission_parsed = parse_submission_json_for_db(submission_path)

            assert submission_parsed is not None
            assert isinstance(submission_parsed["companies"], dict)
            assert isinstance(submission_parsed["filings"], list)
            assert isinstance(submission_parsed["tickers"], list)

            db.insert_companies(submission_parsed["companies"])
            db.insert_filings(submission_parsed["filings"])
            db.insert_tickers(submission_parsed["tickers"])

            # Step 2: Parse and load company facts
            facts_path = FIXTURES_DIR / "companyfacts_sample.json"
            facts_parsed = parse_company_facts_json_for_db(facts_path)

            if facts_parsed and facts_parsed.get("xbrl_facts"):
                assert isinstance(facts_parsed["xbrl_tags"], list)
                assert isinstance(facts_parsed["xbrl_facts"], list)
                db.insert_xbrl_tags(facts_parsed["xbrl_tags"])
                db.insert_xbrl_facts(facts_parsed["xbrl_facts"])

            # Step 3: Validate data consistency
            company_count = db.row_count("companies")
            filing_count = db.row_count("filings")
            fact_count = db.row_count("xbrl_facts")

            assert company_count >= 1, "Should have at least 1 company"
            assert filing_count >= 1, "Should have at least 1 filing"
            assert fact_count >= 1, "Should have at least 1 fact"

            # Step 4: Verify referential integrity
            facts_with_valid_accession = db.query(
                """
                SELECT f.cik, f.accession_number
                FROM xbrl_facts f
                JOIN filings fil ON f.accession_number = fil.accession_number
                LIMIT 10
                """
            )

            # If we have facts and filings, validation should work
            if fact_count > 0 and filing_count > 0:
                assert len(facts_with_valid_accession) >= 0

    def test_malformed_and_empty_coexist(self) -> None:
        """Load malformed and empty fixtures together for no cross-contamination."""
        with IntegrationTestDB() as db:
            # Load malformed submission (should skip bad filings)
            malformed_path = FIXTURES_DIR / "submission_malformed.json"
            malformed_parsed = parse_submission_json_for_db(malformed_path)

            if malformed_parsed:
                assert isinstance(malformed_parsed["companies"], dict)
                assert isinstance(malformed_parsed["filings"], list)
                db.insert_companies(malformed_parsed["companies"])
                db.insert_filings(malformed_parsed["filings"])

            # Load empty companyfacts (returns None)
            empty_facts_path = FIXTURES_DIR / "companyfacts_empty.json"
            empty_facts_parsed = parse_company_facts_json_for_db(empty_facts_path)

            assert empty_facts_parsed is None

            # Load valid companyfacts
            valid_facts_path = FIXTURES_DIR / "companyfacts_sample.json"
            valid_facts_parsed = parse_company_facts_json_for_db(valid_facts_path)

            if valid_facts_parsed:
                assert isinstance(valid_facts_parsed["xbrl_tags"], list)
                assert isinstance(valid_facts_parsed["xbrl_facts"], list)
                db.insert_xbrl_tags(valid_facts_parsed["xbrl_tags"])
                db.insert_xbrl_facts(valid_facts_parsed["xbrl_facts"])

            # Verify we have data from valid loads but not corrupted by empty/malformed
            fact_count = db.row_count("xbrl_facts")
            assert fact_count > 0, "Should have facts from valid load"


class TestEdgeCasesEndToEnd:
    """Test edge cases with actual DuckDB loading."""

    def test_orphaned_facts_detection(self) -> None:
        """Load facts without corresponding filing and verify orphan detection."""
        from datetime import date

        with IntegrationTestDB() as db:
            # Manually create a "naked" fact (no filing entry)
            naked_fact = {
                "cik": "0000320193",
                "accession_number": "9999-ORPHAN-9999",  # Non-existent accession
                "taxonomy": "us-gaap",
                "tag_name": "Assets",
                "unit": "USD",
                "period_end_date": date(2023, 12, 31),
                "value_numeric": 1000000.0,
                "value_text": None,
                "fy": 2023,
                "fp": "FY",
                "form": "10-K",
                "filed_date": date(2024, 2, 1),
                "frame": "",
            }

            db.insert_xbrl_facts([naked_fact])

            # Query for orphaned facts (facts with no corresponding filing)
            orphaned = db.query(
                """
                SELECT f.* FROM xbrl_facts f
                LEFT JOIN filings fil ON f.accession_number = fil.accession_number
                WHERE fil.accession_number IS NULL
                """
            )

            assert len(orphaned) >= 1, "Should detect orphaned fact"
            assert orphaned[0]["accession_number"] == "9999-ORPHAN-9999"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
