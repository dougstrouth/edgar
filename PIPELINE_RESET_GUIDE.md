# Quick Start: Fresh EDGAR Pipeline Run

## Current State Summary

Your database currently has:
- **Stock Data (PRESERVED)**: 599,020 rows in stock_history ✓
- **EDGAR Data**: ~35M rows across companies, filings, xbrl_facts
- **Downloads**: submissions.zip (1.1GB) and companyfacts.zip (453MB) ✓

## Option 1: Clean Reset (Recommended)

If you don't trust the current EDGAR data and want to start fresh:

```bash
# 1. Reset the pipeline (preserves stock data and downloads)
python reset_edgar_pipeline.py

# Type 'YES' when prompted

# 2. Extract JSON from existing ZIP files (fast, no re-download needed)
python main.py extract

# 3. Parse JSON to parquet format
python main.py parse-to-parquet

# 4. Load parquet into database
python main.py load
```

**Time estimate**: 
- Extract: ~5-10 minutes
- Parse: ~15-30 minutes (with MAX_CPU_IO_WORKERS=1)
- Load: ~5-10 minutes

## Option 2: Validate First, Then Decide

Check if your current data is trustworthy:

```bash
# Run database validation
pytest tests/test_database_validation.py -v

# Or validate programmatically
python -c "
import duckdb
from utils.db_validation import validate_edgar_database

conn = duckdb.connect('/Users/dougstrouth/datasets_noicloud/edgar/edgar_analytics.duckdb')
validator = validate_edgar_database(conn)

summary = validator.get_summary()
print(f'\\nValidation: {summary[\"passed\"]}/{summary[\"total\"]} passed ({summary[\"pass_rate\"]:.1f}%)')

# Show failures
failures = [r for r in validator.results if not r.passed]
if failures:
    print(f'\\n{len(failures)} FAILURES:')
    for r in failures[:10]:  # Show first 10
        print(f'  ✗ {r.message}')
else:
    print('\\n✓ All validations passed! Data looks good.')

conn.close()
"
```

If validation passes, you can trust your current data and skip the reset.

## Option 3: Quick Verification Only

Just check what you have without validation:

```bash
python -c "
import duckdb
conn = duckdb.connect('/Users/dougstrouth/datasets_noicloud/edgar/edgar_analytics.duckdb', read_only=True)

# Check EDGAR data completeness
companies = conn.execute('SELECT COUNT(*) FROM companies').fetchone()[0]
filings = conn.execute('SELECT COUNT(*) FROM filings').fetchone()[0]
xbrl = conn.execute('SELECT COUNT(*) FROM xbrl_facts').fetchone()[0]

print(f'EDGAR Data:')
print(f'  Companies: {companies:,}')
print(f'  Filings: {filings:,}')
print(f'  XBRL Facts: {xbrl:,}')

# Check stock data
stock = conn.execute('SELECT COUNT(*) FROM stock_history').fetchone()[0]
print(f'\\nStock Data:')
print(f'  History records: {stock:,}')

# Sample a company to verify data quality
sample = conn.execute('''
    SELECT c.primary_name, COUNT(f.accession_number) as filing_count
    FROM companies c
    LEFT JOIN filings f ON c.cik = f.cik
    WHERE c.cik = '0000320193'  -- Apple
    GROUP BY c.primary_name
''').fetchone()

if sample:
    print(f'\\nSample Check (Apple):')
    print(f'  Name: {sample[0]}')
    print(f'  Filings: {sample[1]:,}')

conn.close()
"
```

## What the Reset Script Does

✓ **Preserves** (keeps your valuable data):
- Stock history data (599,020 rows)
- Macro economic data (47,368 rows)  
- Market risk factors (16,414 rows)
- Downloaded ZIP files (no re-download needed!)

✗ **Removes** (cleans up potentially corrupt data):
- All EDGAR tables (companies, filings, xbrl_facts, etc.)
- Parquet intermediate files
- Extracted JSON files

## After Reset: Full Pipeline

```bash
# Step 1: Extract (unzip the archives you already have)
python main.py extract
# Creates: /Users/dougstrouth/datasets_noicloud/edgar/downloads/extracted_json/

# Step 2: Parse to parquet (intermediate format)
python main.py parse-to-parquet
# Creates: /Users/dougstrouth/datasets_noicloud/edgar/downloads/parquet_data/

# Step 3: Load into database (final step)
python main.py load
# Updates: edgar_analytics.duckdb with fresh EDGAR data

# Step 4: Validate (optional but recommended)
pytest tests/test_database_validation.py -v
```

## Monitoring Progress

Watch the logs during processing:

```bash
# In another terminal
tail -f logs/parse_to_parquet.log
# or
tail -f logs/edgar_data_loader.log
```

## Troubleshooting

If extraction or parsing fails:

```bash
# Check for partial batches
python -c "
from pathlib import Path
from utils.parquet_manager import cleanup_partial_batches

parquet_dir = Path('/Users/dougstrouth/datasets_noicloud/edgar/downloads/parquet_data')
deleted = cleanup_partial_batches(parquet_dir, age_hours=1)
print(f'Cleaned up {deleted} partial batch files')
"
```

## Performance Tuning

In your `.env` file:

```bash
# Slower but more stable (current setting)
MAX_CPU_IO_WORKERS=1

# Faster if you have CPU cores to spare
MAX_CPU_IO_WORKERS=4

# Process in smaller batches for better progress visibility
CIK_BATCH_SIZE=10  # Current: 10 (good for monitoring)
CIK_BATCH_SIZE=100 # Faster but less granular progress
```

## Recommendation

Given your current state with 35M+ rows of EDGAR data, I recommend:

1. **Run validation first** to see if current data is trustworthy
2. **If validation fails or you're unsure**: Run the reset script and start fresh
3. **The reset is safe** - it preserves your stock data and existing downloads

The reset + full pipeline will take ~30-45 minutes total, which is much faster than re-downloading from SEC (would take hours with rate limits).
