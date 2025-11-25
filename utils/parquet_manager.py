# -*- coding: utf-8 -*-
"""
Parquet File Management Utilities

Provides utilities for safe parquet file operations including:
- Atomic batch writes with rollback capability
- Validation of parquet files
- Cleanup of partial/corrupted files
- Recovery from failed writes
"""

import logging
from pathlib import Path
from typing import List, Dict, Set, Optional
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class ParquetBatchManager:
    """Manages safe batch writing of parquet files with rollback capability."""
    
    def __init__(self, base_dir: Path):
        """Initialize batch manager with base directory."""
        self.base_dir = base_dir
        self.current_batch_files: List[Path] = []
        self.batch_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    
    def write_batch_file(
        self, 
        df: pd.DataFrame, 
        subdirectory: str,
        validate: bool = True
    ) -> Optional[Path]:
        """
        Write a dataframe to a parquet file as part of a batch.
        Tracks the file for potential rollback.
        
        Args:
            df: DataFrame to write
            subdirectory: Subdirectory within base_dir (e.g., 'companies', 'filings')
            validate: Whether to validate the file after writing
        
        Returns:
            Path to written file, or None if write failed
        """
        if df.empty:
            logger.debug(f"Skipping empty dataframe for {subdirectory}")
            return None
        
        try:
            dir_path = self.base_dir / subdirectory
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Use batch_id in filename to group related writes
            file_path = dir_path / f"batch_{self.batch_id}_{len(self.current_batch_files):04d}.parquet"
            
            # Write the file
            df.to_parquet(file_path, engine='pyarrow', index=False)
            
            # Validate if requested
            if validate and not self._validate_parquet_file(file_path, expected_rows=len(df)):
                logger.error(f"Validation failed for {file_path}")
                # Remove invalid file
                file_path.unlink(missing_ok=True)
                return None
            
            # Track for potential rollback
            self.current_batch_files.append(file_path)
            logger.debug(f"Successfully wrote and tracked {file_path.name}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to write parquet file to {subdirectory}: {e}", exc_info=True)
            return None
    
    def commit_batch(self) -> bool:
        """
        Commit the current batch by clearing the tracking list.
        Files remain on disk.
        
        Returns:
            True if commit successful
        """
        logger.info(f"Committing batch {self.batch_id} with {len(self.current_batch_files)} files")
        self.current_batch_files.clear()
        return True
    
    def rollback_batch(self) -> bool:
        """
        Rollback the current batch by deleting all tracked files.
        
        Returns:
            True if rollback successful
        """
        logger.warning(f"Rolling back batch {self.batch_id}, deleting {len(self.current_batch_files)} files")
        
        success = True
        for file_path in self.current_batch_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(f"Deleted {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete {file_path} during rollback: {e}")
                success = False
        
        self.current_batch_files.clear()
        return success
    
    @staticmethod
    def _validate_parquet_file(file_path: Path, expected_rows: Optional[int] = None) -> bool:
        """
        Validate that a parquet file is readable and has expected properties.
        
        Args:
            file_path: Path to parquet file
            expected_rows: Expected number of rows (optional)
        
        Returns:
            True if valid, False otherwise
        """
        try:
            # Try to read the file
            df = pd.read_parquet(file_path)
            
            # Check row count if expected
            if expected_rows is not None and len(df) != expected_rows:
                logger.error(f"Row count mismatch: expected {expected_rows}, got {len(df)}")
                return False
            
            # File is readable
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate {file_path}: {e}")
            return False


def cleanup_partial_batches(parquet_dir: Path, age_hours: int = 24) -> int:
    """
    Clean up incomplete or orphaned batch files older than specified age.
    
    This is a safety mechanism to clean up files from failed runs.
    Files are only deleted if they appear to be from incomplete batches.
    
    Args:
        parquet_dir: Base parquet directory
        age_hours: Delete partial batches older than this many hours
    
    Returns:
        Number of files deleted
    """
    from datetime import datetime, timedelta
    
    deleted_count = 0
    cutoff_time = datetime.now() - timedelta(hours=age_hours)
    
    logger.info(f"Scanning for partial batch files older than {age_hours} hours in {parquet_dir}")
    
    # Scan all subdirectories
    for subdir in parquet_dir.iterdir():
        if not subdir.is_dir():
            continue
        
        # Group files by batch_id
        batch_groups: Dict[str, List[Path]] = {}
        
        for parquet_file in subdir.glob("batch_*.parquet"):
            # Extract batch_id from filename (format: batch_YYYYMMDD_HHMMSS_ffffff_NNNN.parquet)
            parts = parquet_file.stem.split('_')
            if len(parts) >= 4:
                batch_id = '_'.join(parts[1:4])  # YYYYMMDD_HHMMSS_ffffff
                
                if batch_id not in batch_groups:
                    batch_groups[batch_id] = []
                batch_groups[batch_id].append(parquet_file)
        
        # Check each batch group
        for batch_id, files in batch_groups.items():
            # Parse batch timestamp
            try:
                # batch_id format: YYYYMMDD_HHMMSS_ffffff
                timestamp_str = batch_id.split('_')
                if len(timestamp_str) >= 2:
                    # Just use date and time, ignore microseconds for age check
                    batch_time = datetime.strptime(f"{timestamp_str[0]}_{timestamp_str[1]}", "%Y%m%d_%H%M%S")
                    
                    # If batch is old and appears incomplete (e.g., only 1-2 files), mark for deletion
                    if batch_time < cutoff_time and len(files) < 3:
                        logger.warning(f"Found potentially incomplete batch {batch_id} with {len(files)} files, deleting")
                        
                        for file_path in files:
                            try:
                                file_path.unlink()
                                deleted_count += 1
                                logger.debug(f"Deleted {file_path}")
                            except Exception as e:
                                logger.error(f"Failed to delete {file_path}: {e}")
            
            except Exception as e:
                logger.debug(f"Could not parse batch timestamp for {batch_id}: {e}")
                continue
    
    if deleted_count > 0:
        logger.info(f"Cleaned up {deleted_count} partial batch files")
    else:
        logger.info("No partial batch files found to clean up")
    
    return deleted_count


def validate_parquet_directory(parquet_dir: Path, table_name: str) -> Dict[str, any]:
    """
    Validate all parquet files in a directory.
    
    Args:
        parquet_dir: Directory containing parquet files
        table_name: Name of the table (for logging)
    
    Returns:
        Dictionary with validation results
    """
    results = {
        "total_files": 0,
        "valid_files": 0,
        "invalid_files": 0,
        "total_rows": 0,
        "errors": []
    }
    
    table_dir = parquet_dir / table_name
    if not table_dir.exists():
        logger.warning(f"Directory does not exist: {table_dir}")
        return results
    
    for parquet_file in table_dir.glob("*.parquet"):
        results["total_files"] += 1
        
        try:
            df = pd.read_parquet(parquet_file)
            results["valid_files"] += 1
            results["total_rows"] += len(df)
            
        except Exception as e:
            results["invalid_files"] += 1
            error_msg = f"Invalid file {parquet_file.name}: {str(e)}"
            results["errors"].append(error_msg)
            logger.error(error_msg)
    
    logger.info(f"Validated {table_name}: {results['valid_files']}/{results['total_files']} files valid, "
                f"{results['total_rows']} total rows")
    
    return results


def get_batch_ids_in_directory(parquet_dir: Path, table_name: str) -> Set[str]:
    """
    Get all unique batch IDs from parquet files in a directory.
    
    Args:
        parquet_dir: Base parquet directory
        table_name: Table subdirectory name
    
    Returns:
        Set of batch IDs found
    """
    batch_ids: Set[str] = set()
    table_dir = parquet_dir / table_name
    
    if not table_dir.exists():
        return batch_ids
    
    for parquet_file in table_dir.glob("batch_*.parquet"):
        parts = parquet_file.stem.split('_')
        if len(parts) >= 4:
            batch_id = '_'.join(parts[1:4])
            batch_ids.add(batch_id)
    
    return batch_ids
