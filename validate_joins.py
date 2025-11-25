#!/usr/bin/env python3
"""
Validate database joins and data relationships.

This script verifies that:
1. CIK formats are consistent across tables
2. Joins between tables work correctly
3. Data relationships are properly established
"""

import duckdb
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig

def validate_joins():
    """Validate all critical joins in the database."""
    config = AppConfig(calling_script_path=Path(__file__))
    con = duckdb.connect(str(config.DB_FILE), read_only=True)
    
    print("="*80)
    print("DATABASE JOIN VALIDATION REPORT")
    print("="*80)
    
    # 1. Check CIK consistency
    print("\n1. CIK FORMAT CONSISTENCY")
    print("-" * 80)
    
    tables_with_cik = ['companies', 'tickers', 'filings', 'xbrl_facts']
    for table in tables_with_cik:
        try:
            result = con.execute(f"""
                SELECT 
                    MIN(LENGTH(cik)) as min_len,
                    MAX(LENGTH(cik)) as max_len,
                    COUNT(DISTINCT cik) as unique_ciks,
                    COUNT(*) as total_rows
                FROM {table}
            """).fetchone()
            print(f"  {table:20} Min CIK len: {result[0]:2}  Max CIK len: {result[1]:2}  "
                  f"Unique CIKs: {result[2]:5}  Total rows: {result[3]:,}")
        except Exception as e:
            print(f"  {table:20} Error: {e}")
    
    # 2. Test ticker -> company join
    print("\n2. TICKERS -> COMPANIES JOIN")
    print("-" * 80)
    result = con.execute("""
        SELECT 
            COUNT(DISTINCT t.ticker) as tickers,
            COUNT(DISTINCT c.cik) as companies_matched,
            COUNT(DISTINCT t.cik) as total_ciks_in_tickers
        FROM tickers t
        LEFT JOIN companies c ON t.cik = c.cik
    """).fetchone()
    print(f"  Total tickers: {result[0]}")
    print(f"  Matched to companies: {result[1]}")
    print(f"  Total unique CIKs in tickers: {result[2]}")
    print(f"  Join success rate: {(result[1]/result[2]*100):.1f}%")
    
    # 3. Test ticker -> filings join
    print("\n3. TICKERS -> FILINGS JOIN")
    print("-" * 80)
    result = con.execute("""
        SELECT 
            t.ticker,
            t.cik,
            COUNT(f.accession_number) as filing_count
        FROM tickers t
        LEFT JOIN filings f ON t.cik = f.cik
        GROUP BY t.ticker, t.cik
        ORDER BY filing_count DESC
    """).fetchall()
    for row in result:
        print(f"  {row[0]:10} (CIK: {row[1]})  Filings: {row[2]:5,}")
    
    # 4. Test ticker -> xbrl_facts join
    print("\n4. TICKERS -> XBRL_FACTS JOIN")
    print("-" * 80)
    result = con.execute("""
        SELECT 
            t.ticker,
            t.cik,
            COUNT(DISTINCT f.tag_name) as unique_tags,
            COUNT(*) as total_facts
        FROM tickers t
        LEFT JOIN xbrl_facts f ON t.cik = f.cik
        GROUP BY t.ticker, t.cik
        ORDER BY unique_tags DESC
    """).fetchall()
    
    with_xbrl = 0
    without_xbrl = 0
    for row in result:
        if row[2] > 0:
            with_xbrl += 1
            print(f"  ✓ {row[0]:10} (CIK: {row[1]})  Tags: {row[2]:4}  Facts: {row[3]:,}")
        else:
            without_xbrl += 1
            print(f"  ✗ {row[0]:10} (CIK: {row[1]})  NO XBRL DATA")
    
    print(f"\n  Summary: {with_xbrl} tickers with XBRL data, {without_xbrl} without")
    
    # 5. Test filings -> xbrl_facts join
    print("\n5. FILINGS -> XBRL_FACTS JOIN")
    print("-" * 80)
    result = con.execute("""
        SELECT 
            f.cik,
            COUNT(DISTINCT f.accession_number) as total_filings,
            COUNT(DISTINCT x.accession_number) as filings_with_xbrl,
            COUNT(DISTINCT x.tag_name) as unique_tags
        FROM filings f
        LEFT JOIN xbrl_facts x ON f.cik = x.cik AND f.accession_number = x.accession_number
        GROUP BY f.cik
        ORDER BY filings_with_xbrl DESC
    """).fetchall()
    
    for row in result:
        ticker = con.execute("SELECT ticker FROM tickers WHERE cik = ? LIMIT 1", [row[0]]).fetchone()
        ticker_str = ticker[0] if ticker else "unknown"
        pct = (row[2]/row[1]*100) if row[1] > 0 else 0
        print(f"  CIK {row[0]} ({ticker_str:10})  Total filings: {row[1]:5}  "
              f"With XBRL: {row[2]:5} ({pct:5.1f}%)  Tags: {row[3]:4}")
    
    # 6. Check for potential CIK format issues
    print("\n6. POTENTIAL CIK FORMAT ISSUES")
    print("-" * 80)
    
    # Check if any CIKs in one table don't match the other
    orphan_ciks = con.execute("""
        SELECT DISTINCT f.cik
        FROM filings f
        WHERE NOT EXISTS (
            SELECT 1 FROM companies c WHERE c.cik = f.cik
        )
    """).fetchall()
    
    if orphan_ciks:
        print(f"  ⚠ Found {len(orphan_ciks)} CIKs in filings that don't match companies:")
        for row in orphan_ciks[:5]:
            print(f"    {row[0]}")
    else:
        print("  ✓ All filings CIKs match companies table")
    
    # 7. Data completeness summary
    print("\n7. DATA COMPLETENESS SUMMARY")
    print("-" * 80)
    
    tables = {
        'companies': 'CIK',
        'tickers': 'ticker',
        'filings': 'accession_number',
        'xbrl_facts': 'tag_name',
        'xbrl_tags': 'tag_name',
        'stock_history': 'ticker'
    }
    
    for table, key in tables.items():
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:20} {count:,} rows")
        except Exception as e:
            print(f"  {table:20} Error: {e}")
    
    # 8. Recommendation
    print("\n8. RECOMMENDATIONS")
    print("-" * 80)
    
    xbrl_ciks = con.execute("SELECT COUNT(DISTINCT cik) FROM xbrl_facts").fetchone()[0]
    total_ciks = con.execute("SELECT COUNT(DISTINCT cik) FROM companies").fetchone()[0]
    
    if xbrl_ciks < total_ciks:
        missing = total_ciks - xbrl_ciks
        print(f"  ⚠ You have XBRL data for only {xbrl_ciks}/{total_ciks} companies.")
        print(f"    {missing} companies are missing XBRL data.")
        print(f"\n  To load XBRL data for all companies:")
        print(f"    1. Fetch companyfacts archives: python main.py fetch")
        print(f"    2. Parse to parquet: python main.py parse-to-parquet")
        print(f"    3. Load to database: python main.py load")
    else:
        print(f"  ✓ All {total_ciks} companies have XBRL data loaded!")
    
    print("\n" + "="*80)
    print("VALIDATION COMPLETE")
    print("="*80)
    
    con.close()

if __name__ == "__main__":
    validate_joins()
