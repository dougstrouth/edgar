# -*- coding: utf-8 -*-
"""
Database Validation Tests

Comprehensive tests for database schema, data integrity, and referential constraints.
Uses the DatabaseValidator utility to ensure EDGAR database quality.
"""

import pytest
from utils.db_validation import DatabaseValidator, validate_edgar_database, ValidationResult


class TestDatabaseValidator:
    """Test the DatabaseValidator class functionality."""
    
    def test_validator_initialization(self, in_memory_db):
        """Test that validator initializes correctly."""
        validator = DatabaseValidator(in_memory_db)
        assert validator.conn is not None
        assert len(validator.results) == 0
    
    def test_get_summary(self, db_validator):
        """Test summary generation."""
        # Add some mock results
        db_validator.results = [
            ValidationResult(True, "Test 1"),
            ValidationResult(True, "Test 2"),
            ValidationResult(False, "Test 3"),
        ]
        
        summary = db_validator.get_summary()
        assert summary["total"] == 3
        assert summary["passed"] == 2
        assert summary["failed"] == 1
        assert summary["pass_rate"] == pytest.approx(66.67, rel=0.01)
    
    def test_reset_results(self, db_validator):
        """Test results can be reset."""
        db_validator.results = [ValidationResult(True, "Test")]
        assert len(db_validator.results) == 1
        
        db_validator.reset_results()
        assert len(db_validator.results) == 0


class TestSchemaValidation:
    """Test schema validation functions."""
    
    def test_validate_table_exists_success(self, sample_edgar_db, db_validator):
        """Test detection of existing table."""
        result = db_validator.validate_table_exists("companies")
        assert result.passed
        assert "companies" in result.message
    
    def test_validate_table_exists_failure(self, db_validator):
        """Test detection of missing table."""
        result = db_validator.validate_table_exists("nonexistent_table")
        assert not result.passed
        assert "does not exist" in result.message
    
    def test_validate_columns_success(self, sample_edgar_db, db_validator):
        """Test column validation with correct columns."""
        result = db_validator.validate_columns("companies", [
            "cik", "primary_name", "entity_type", "sic", "sic_description", "entity_name_cf"
        ])
        assert result.passed
    
    def test_validate_columns_missing(self, sample_edgar_db, db_validator):
        """Test detection of missing columns."""
        result = db_validator.validate_columns("companies", ["cik", "nonexistent_column"])
        assert not result.passed
        assert "missing" in result.details
        assert "nonexistent_column" in result.details["missing"]
    
    def test_validate_columns_extra(self, sample_edgar_db, db_validator):
        """Test detection of extra columns."""
        result = db_validator.validate_columns("companies", ["cik"])  # Missing other columns
        assert not result.passed
        assert "extra" in result.details
    
    def test_validate_primary_key_success(self, sample_edgar_db, db_validator):
        """Test primary key validation."""
        result = db_validator.validate_primary_key("companies", ["cik"])
        assert result.passed


class TestDataIntegrityValidation:
    """Test data integrity validation functions."""
    
    def test_validate_no_duplicates_success(self, sample_edgar_db, db_validator):
        """Test no duplicates detection when data is clean."""
        result = db_validator.validate_no_duplicates("companies", ["cik"])
        assert result.passed
    
    def test_validate_no_duplicates_failure(self, in_memory_db, db_validator):
        """Test duplicate detection."""
        # Create table with duplicates
        in_memory_db.execute("""
            CREATE TABLE test_dupes (id INT, name VARCHAR)
        """)
        in_memory_db.execute("""
            INSERT INTO test_dupes VALUES (1, 'A'), (1, 'A'), (2, 'B')
        """)
        
        result = db_validator.validate_no_duplicates("test_dupes", ["id", "name"])
        assert not result.passed
        assert result.details["duplicate_count"] > 0
    
    def test_validate_no_nulls_success(self, sample_edgar_db, db_validator):
        """Test NULL detection when no NULLs present."""
        result = db_validator.validate_no_nulls("companies", "cik")
        assert result.passed
    
    def test_validate_no_nulls_failure(self, in_memory_db, db_validator):
        """Test NULL detection when NULLs are present."""
        in_memory_db.execute("""
            CREATE TABLE test_nulls (id INT, name VARCHAR)
        """)
        in_memory_db.execute("""
            INSERT INTO test_nulls VALUES (1, NULL), (2, 'B')
        """)
        
        result = db_validator.validate_no_nulls("test_nulls", "name")
        assert not result.passed
        assert result.details["null_count"] == 1


class TestReferentialIntegrityValidation:
    """Test referential integrity validation functions."""
    
    def test_validate_foreign_key_success(self, sample_edgar_db, db_validator):
        """Test FK validation when all references are valid."""
        result = db_validator.validate_foreign_key("filings", "cik", "companies", "cik")
        assert result.passed
    
    def test_validate_foreign_key_failure(self, in_memory_db, db_validator):
        """Test FK validation with orphaned records."""
        # Create tables
        in_memory_db.execute("""
            CREATE TABLE parent (id INT PRIMARY KEY)
        """)
        in_memory_db.execute("""
            CREATE TABLE child (id INT, parent_id INT)
        """)
        
        # Insert data with orphan
        in_memory_db.execute("INSERT INTO parent VALUES (1)")
        in_memory_db.execute("INSERT INTO child VALUES (1, 1), (2, 999)")  # 999 is orphaned
        
        result = db_validator.validate_foreign_key("child", "parent_id", "parent", "id")
        assert not result.passed
        assert result.details["orphan_count"] == 1
        assert 999 in result.details["sample_orphaned_values"]
    
    def test_validate_foreign_key_with_nulls_allowed(self, in_memory_db, db_validator):
        """Test FK validation allowing NULL values."""
        in_memory_db.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        in_memory_db.execute("CREATE TABLE child (id INT, parent_id INT)")
        
        in_memory_db.execute("INSERT INTO parent VALUES (1)")
        in_memory_db.execute("INSERT INTO child VALUES (1, 1), (2, NULL)")
        
        # With allow_null=True, NULL values should be ignored
        result = db_validator.validate_foreign_key("child", "parent_id", "parent", "id", allow_null=True)
        assert result.passed
        
        # Without allow_null, NULL values should cause failure
        result_strict = db_validator.validate_foreign_key("child", "parent_id", "parent", "id", allow_null=False)
        assert not result_strict.passed


class TestDataQualityValidation:
    """Test data quality validation functions."""
    
    def test_validate_value_range_success(self, in_memory_db, db_validator):
        """Test value range validation when values are in range."""
        in_memory_db.execute("CREATE TABLE test_range (value INT)")
        in_memory_db.execute("INSERT INTO test_range VALUES (5), (10), (15)")
        
        result = db_validator.validate_value_range("test_range", "value", min_val=0, max_val=20)
        assert result.passed
    
    def test_validate_value_range_failure(self, in_memory_db, db_validator):
        """Test value range validation with out-of-range values."""
        in_memory_db.execute("CREATE TABLE test_range (value INT)")
        in_memory_db.execute("INSERT INTO test_range VALUES (5), (10), (25)")  # 25 is out of range
        
        result = db_validator.validate_value_range("test_range", "value", min_val=0, max_val=20)
        assert not result.passed
        assert result.details["out_of_range_count"] == 1
    
    def test_validate_row_count_success(self, sample_edgar_db, db_validator):
        """Test row count validation when in range."""
        result = db_validator.validate_row_count("companies", min_rows=1, max_rows=10)
        assert result.passed
    
    def test_validate_row_count_too_few(self, in_memory_db, db_validator):
        """Test row count validation with too few rows."""
        in_memory_db.execute("CREATE TABLE test_count (id INT)")
        in_memory_db.execute("INSERT INTO test_count VALUES (1)")
        
        result = db_validator.validate_row_count("test_count", min_rows=5)
        assert not result.passed
        assert "below minimum" in result.message
    
    def test_validate_row_count_too_many(self, in_memory_db, db_validator):
        """Test row count validation with too many rows."""
        in_memory_db.execute("CREATE TABLE test_count (id INT)")
        in_memory_db.execute("INSERT INTO test_count VALUES (1), (2), (3), (4), (5), (6)")
        
        result = db_validator.validate_row_count("test_count", max_rows=3)
        assert not result.passed
        assert "above maximum" in result.message
    
    def test_validate_date_consistency_success(self, in_memory_db, db_validator):
        """Test date consistency validation with valid dates."""
        in_memory_db.execute("CREATE TABLE test_dates (start_date DATE, end_date DATE)")
        in_memory_db.execute("""
            INSERT INTO test_dates VALUES 
                ('2023-01-01', '2023-12-31'),
                ('2024-01-01', '2024-06-30')
        """)
        
        result = db_validator.validate_date_consistency("test_dates", "start_date", "end_date")
        assert result.passed
    
    def test_validate_date_consistency_failure(self, in_memory_db, db_validator):
        """Test date consistency validation with invalid dates."""
        in_memory_db.execute("CREATE TABLE test_dates (start_date DATE, end_date DATE)")
        in_memory_db.execute("""
            INSERT INTO test_dates VALUES 
                ('2023-12-31', '2023-01-01'),  -- Invalid: end before start
                ('2024-01-01', '2024-06-30')
        """)
        
        result = db_validator.validate_date_consistency("test_dates", "start_date", "end_date")
        assert not result.passed
        assert result.details["invalid_count"] == 1


class TestEDGARDatabaseValidation:
    """Test comprehensive EDGAR database validation."""
    
    @pytest.mark.integration
    def test_validate_edgar_database_comprehensive(self, sample_edgar_db):
        """Test full EDGAR database validation suite."""
        validator = validate_edgar_database(sample_edgar_db)
        
        # Should have run multiple validations
        assert len(validator.results) > 0
        
        # Get summary
        summary = validator.get_summary()
        assert summary["total"] > 0
        
        # Most validations should pass on sample data
        assert summary["pass_rate"] > 50  # At least 50% should pass
    
    def test_validation_catches_missing_tables(self, in_memory_db):
        """Test that validation catches missing tables."""
        validator = validate_edgar_database(in_memory_db)
        
        # Should have failures for missing tables
        summary = validator.get_summary()
        assert summary["failed"] > 0
        
        # Check that specific table validations failed
        table_results = [r for r in validator.results if "does not exist" in r.message]
        assert len(table_results) > 0


class TestValidationResultFormatting:
    """Test ValidationResult formatting and display."""
    
    def test_validation_result_str_pass(self):
        """Test string representation of passing result."""
        result = ValidationResult(True, "Test passed")
        assert "✓ PASS" in str(result)
        assert "Test passed" in str(result)
    
    def test_validation_result_str_fail(self):
        """Test string representation of failing result."""
        result = ValidationResult(False, "Test failed")
        assert "✗ FAIL" in str(result)
        assert "Test failed" in str(result)
    
    def test_validation_result_with_details(self):
        """Test result with details."""
        result = ValidationResult(
            False, 
            "Test failed",
            {"count": 5, "samples": ["a", "b"]}
        )
        assert result.details is not None
        assert result.details["count"] == 5
        assert len(result.details["samples"]) == 2


@pytest.mark.requires_db
class TestDatabaseValidationIntegration:
    """Integration tests for database validation against real database."""
    
    def test_validation_results_are_logged(self, sample_edgar_db, caplog):
        """Test that validation logs information."""
        import logging
        caplog.set_level(logging.INFO)
        
        _ = validate_edgar_database(sample_edgar_db)
        
        # Should have logged validation steps
        assert len(caplog.records) > 0
    
    def test_validator_handles_errors_gracefully(self, in_memory_db, db_validator):
        """Test that validator handles database errors gracefully."""
        # Try to validate a table that doesn't exist
        result = db_validator.validate_columns("nonexistent", ["col1"])
        
        # Should return a failure result, not raise exception
        assert not result.passed
        # The message might be about column mismatch (missing columns) or table not existing
        assert "mismatch" in result.message or "does not exist" in result.message or "Error" in result.message
