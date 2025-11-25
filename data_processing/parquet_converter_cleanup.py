from pathlib import Path

def clean_obsolete_parquet_files(parquet_dir: Path, valid_ciks: set[str]) -> None:
    """
    Remove Parquet files for CIKs not present in valid_ciks from the companies and filings directories.
    """
    import os
    companies_dir = parquet_dir / "companies"
    filings_dir = parquet_dir / "filings"
    # Remove obsolete company Parquet files
    if companies_dir.is_dir():
        for file_path in companies_dir.glob("*.parquet"):
            cik = file_path.stem.replace("CIK", "")
            if cik not in valid_ciks:
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Failed to remove obsolete company Parquet: {file_path}: {e}")
    # Remove obsolete filings Parquet files
    if filings_dir.is_dir():
        for file_path in filings_dir.glob("*.parquet"):
            cik = file_path.stem.split("_")[0].replace("CIK", "")
            if cik not in valid_ciks:
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Failed to remove obsolete filings Parquet: {file_path}: {e}")
