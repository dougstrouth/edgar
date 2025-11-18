#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YFinance Capacity Test Script

Tests how much data can be gathered from YFinance before hitting rate limits.
Uses the prioritizer to get the most valuable tickers first, then gradually
increases batch sizes to find the optimal gathering rate.

Usage:
    python test_yfinance_capacity.py --test-type [quick|medium|extensive]
"""

import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Any

import yfinance as yf  # type: ignore

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig
from utils.logging_utils import setup_logging
from utils.database_conn import ManagedDatabaseConnection
from utils.prioritizer import prioritize_tickers_hybrid

# Setup logging
SCRIPT_NAME = Path(__file__).stem
LOG_DIRECTORY = Path(__file__).resolve().parent / "logs"
logger = setup_logging(SCRIPT_NAME, LOG_DIRECTORY, level=logging.INFO)


class YFinanceCapacityTester:
    """Tests YFinance API capacity with incremental batch sizes."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.results: List[Dict] = []
        self.rate_limit_hit = False
        self.consecutive_failures = 0
        
    def get_prioritized_tickers(self, limit: int = 500) -> List[str]:
        """Get top prioritized tickers from the database."""
        logger.info(f"Getting top {limit} prioritized tickers...")
        
        with ManagedDatabaseConnection(
            db_path_override=self.config.DB_FILE_STR,
            read_only=True
        ) as conn:
            if not conn:
                logger.error("Failed to connect to database")
                return []
            
            # Get all unique tickers
            query = "SELECT DISTINCT ticker FROM tickers WHERE ticker IS NOT NULL ORDER BY ticker;"
            tickers_df = conn.execute(query).df()
            all_tickers = tickers_df['ticker'].tolist()
            
            logger.info(f"Found {len(all_tickers)} total tickers in database")
            
            if not all_tickers:
                return []
            
            # Prioritize them
            try:
                prioritized = prioritize_tickers_hybrid(
                    db_path=self.config.DB_FILE_STR,
                    tickers=all_tickers[:limit],  # Limit to avoid huge queries
                    lookback_days=365
                )
                # Return just the ticker symbols
                return [ticker for ticker, score in prioritized]
            except Exception as e:
                logger.warning(f"Prioritization failed: {e}. Using simple list.")
                return all_tickers[:limit]
    
    def test_single_ticker(self, ticker: str, data_type: str = "history") -> Tuple[bool, float, str]:
        """
        Test fetching data for a single ticker.
        
        Returns: (success, elapsed_time, error_message)
        """
        start_time = time.time()
        error_msg = ""
        
        try:
            if data_type == "history":
                # Try to fetch 5 days of history
                df = yf.Ticker(ticker).history(period="5d")
                success = not df.empty
                if not success:
                    error_msg = "Empty dataframe - ticker may not exist or have no recent data"
                    logger.debug(f"{ticker}: Empty dataframe returned")
            elif data_type == "info":
                # Try to fetch financial statements
                tick = yf.Ticker(ticker)
                _ = tick.income_stmt
                success = True
            else:
                raise ValueError(f"Unknown data_type: {data_type}")
                
        except Exception as e:
            success = False
            error_msg = f"{type(e).__name__}: {str(e)}"
            error_lower = error_msg.lower()
            
            # Check if it's a rate limit error (be specific!)
            if "429" in error_msg or "too many requests" in error_lower or "rate limit" in error_lower:
                self.rate_limit_hit = True
                logger.warning(f"Rate limit hit on ticker {ticker}: {error_msg}")
            else:
                logger.debug(f"{ticker} failed: {error_msg}")
        
        elapsed = time.time() - start_time
        return success, elapsed, error_msg
    
    def test_batch(
        self,
        tickers: List[str],
        batch_size: int,
        delay_between_requests: float = 0.1,
        data_type: str = "history"
    ) -> Dict:
        """
        Test fetching a batch of tickers with specified parameters.
        
        Returns dictionary with batch statistics.
        """
        logger.info(f"Testing batch of {batch_size} tickers with {delay_between_requests}s delay")
        
        batch_tickers = tickers[:batch_size]
        successes = 0
        failures = 0
        total_time = 0.0
        errors = []
        
        batch_start = time.time()
        
        for i, ticker in enumerate(batch_tickers, 1):
            success, elapsed, error = self.test_single_ticker(ticker, data_type)
            total_time += elapsed
            
            if success:
                successes += 1
                self.consecutive_failures = 0
                logger.debug(f"{ticker} succeeded in {elapsed:.2f}s")
            else:
                failures += 1
                self.consecutive_failures += 1
                errors.append(f"{ticker}: {error}")
                logger.info(f"{ticker} failed: {error}")  # Changed to INFO to see failures
                
            # Check if we should stop early
            if self.rate_limit_hit:
                logger.warning(f"Stopping batch early due to rate limit (tested {i}/{batch_size})")
                break
            
            if self.consecutive_failures >= 5:
                logger.warning("Stopping batch early due to 5 consecutive failures")
                break
            
            # Add delay between requests
            if i < len(batch_tickers):
                time.sleep(delay_between_requests)
        
        batch_elapsed = time.time() - batch_start
        
        result: Dict[str, Any] = {
            'batch_size': batch_size,
            'tickers_tested': len(batch_tickers),
            'successes': successes,
            'failures': failures,
            'success_rate': successes / len(batch_tickers) if batch_tickers else 0,
            'total_time': batch_elapsed,
            'avg_time_per_ticker': total_time / len(batch_tickers) if batch_tickers else 0,
            'delay': delay_between_requests,
            'data_type': data_type,
            'rate_limited': self.rate_limit_hit,
            'errors_sample': errors[:5]  # Keep first 5 errors
        }
        
        self.results.append(result)
        return result
    
    def run_quick_test(self, tickers: List[str]):
        """Quick test: 3 small batches with different delays."""
        logger.info("=== Running QUICK capacity test ===")
        
        test_configs: List[Dict[str, Any]] = [
            {'batch_size': 5, 'delay': 0.1},
            {'batch_size': 10, 'delay': 0.2},
            {'batch_size': 15, 'delay': 0.5},
        ]
        
        offset = 0
        for config in test_configs:
            if self.rate_limit_hit:
                logger.warning("Rate limit detected, stopping quick test")
                break
            
            batch_size = int(config['batch_size'])
            batch = tickers[offset:offset + batch_size]
            result = self.test_batch(batch, batch_size, float(config['delay']))
            offset += batch_size
            
            logger.info(
                f"Batch result: {result['successes']}/{result['tickers_tested']} "
                f"success, avg {result['avg_time_per_ticker']:.2f}s per ticker"
            )
            
            # Wait between batches
            if not self.rate_limit_hit:
                time.sleep(2)
    
    def run_medium_test(self, tickers: List[str]):
        """Medium test: Gradual increase in batch size."""
        logger.info("=== Running MEDIUM capacity test ===")
        
        test_configs: List[Dict[str, Any]] = [
            {'batch_size': 10, 'delay': 0.1},
            {'batch_size': 20, 'delay': 0.2},
            {'batch_size': 30, 'delay': 0.3},
            {'batch_size': 50, 'delay': 0.5},
        ]
        
        offset = 0
        for config in test_configs:
            if self.rate_limit_hit:
                logger.warning("Rate limit detected, stopping medium test")
                break
            
            batch_size = int(config['batch_size'])
            batch = tickers[offset:offset + batch_size]
            result = self.test_batch(batch, batch_size, float(config['delay']))
            offset += batch_size
            
            logger.info(
                f"Batch result: {result['successes']}/{result['tickers_tested']} "
                f"success, avg {result['avg_time_per_ticker']:.2f}s per ticker"
            )
            
            # Longer wait between batches for medium test
            if not self.rate_limit_hit:
                time.sleep(5)
    
    def run_extensive_test(self, tickers: List[str]):
        """Extensive test: Try to find the maximum sustainable rate."""
        logger.info("=== Running EXTENSIVE capacity test ===")
        
        # Start conservative, gradually increase
        batch_sizes = [10, 25, 50, 75, 100, 150, 200]
        base_delay = 0.1
        
        offset = 0
        for batch_size in batch_sizes:
            if self.rate_limit_hit:
                logger.warning("Rate limit detected, stopping extensive test")
                break
            
            # Calculate adaptive delay (more delay for larger batches)
            delay = base_delay * (1.0 + (batch_size / 100.0))
            
            batch = tickers[offset:offset + batch_size]
            result = self.test_batch(batch, batch_size, delay)
            offset += batch_size
            
            logger.info(
                f"Batch {batch_size} result: {result['successes']}/{result['tickers_tested']} "
                f"success ({result['success_rate']:.1%}), "
                f"avg {result['avg_time_per_ticker']:.2f}s per ticker"
            )
            
            # If success rate drops below 80%, slow down
            if result['success_rate'] < 0.8:
                logger.warning(f"Success rate dropped to {result['success_rate']:.1%}, increasing delay")
                base_delay *= 1.5
            
            # Wait between batches
            if not self.rate_limit_hit:
                wait_time = 10 + (batch_size / 10)  # Longer wait for bigger batches
                logger.info(f"Waiting {wait_time:.0f}s before next batch...")
                time.sleep(wait_time)
    
    def print_summary(self):
        """Print summary of all test results."""
        if not self.results:
            logger.warning("No test results to summarize")
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("YFINANCE CAPACITY TEST SUMMARY")
        logger.info("=" * 80)
        
        total_tested = sum(r['tickers_tested'] for r in self.results)
        total_successes = sum(r['successes'] for r in self.results)
        total_time = sum(r['total_time'] for r in self.results)
        
        logger.info(f"Total tickers tested: {total_tested}")
        logger.info(f"Total successes: {total_successes}")
        logger.info(f"Overall success rate: {total_successes/total_tested:.1%}")
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Average time per ticker: {total_time/total_tested:.2f}s")
        logger.info(f"Rate limit hit: {self.rate_limit_hit}")
        
        logger.info("\n" + "-" * 80)
        logger.info("Per-batch breakdown:")
        logger.info("-" * 80)
        
        for i, result in enumerate(self.results, 1):
            logger.info(
                f"Batch {i}: {result['batch_size']} tickers, "
                f"{result['success_rate']:.1%} success, "
                f"{result['delay']:.2f}s delay, "
                f"{result['avg_time_per_ticker']:.2f}s avg"
            )
        
        logger.info("\n" + "-" * 80)
        logger.info("RECOMMENDATIONS:")
        logger.info("-" * 80)
        
        # Calculate recommended settings based on results
        successful_batches = [r for r in self.results if r['success_rate'] >= 0.8]
        
        if successful_batches:
            avg_time = sum(r['avg_time_per_ticker'] for r in successful_batches) / len(successful_batches)
            max_successful_batch = max(r['batch_size'] for r in successful_batches)
            
            logger.info(f"1. Maximum reliable batch size: {max_successful_batch} tickers")
            logger.info(f"2. Average time per ticker: {avg_time:.2f}s")
            logger.info(f"3. Estimated tickers per hour: {int(3600 / avg_time)}")
            
            if self.rate_limit_hit:
                logger.info("4. Rate limit WAS hit - use conservative settings")
                logger.info("   Recommended: Small batches with longer delays")
            else:
                logger.info("4. Rate limit NOT hit - can potentially go faster")
                logger.info("   Recommended: Test with slightly larger batches")
        else:
            logger.info("All batches had poor success rates. YFinance may be unstable.")
            logger.info("Recommendation: Use YFINANCE_DISABLED=1 for now")


def main():
    parser = argparse.ArgumentParser(description="Test YFinance API capacity")
    parser.add_argument(
        '--test-type',
        choices=['quick', 'medium', 'extensive'],
        default='quick',
        help='Type of test to run (default: quick)'
    )
    parser.add_argument(
        '--max-tickers',
        type=int,
        default=500,
        help='Maximum number of tickers to load for testing (default: 500)'
    )
    
    args = parser.parse_args()
    
    try:
        config = AppConfig(calling_script_path=Path(__file__))
    except SystemExit as e:
        logger.critical(f"Configuration failed: {e}")
        sys.exit(1)
    
    # Create tester
    tester = YFinanceCapacityTester(config)
    
    # Get prioritized tickers
    tickers = tester.get_prioritized_tickers(limit=args.max_tickers)
    
    if not tickers:
        logger.error("No tickers found in database. Run 'main.py fetch' first.")
        sys.exit(1)
    
    logger.info(f"Loaded {len(tickers)} prioritized tickers for testing")
    
    # Run appropriate test
    test_start = time.time()
    
    if args.test_type == 'quick':
        tester.run_quick_test(tickers)
    elif args.test_type == 'medium':
        tester.run_medium_test(tickers)
    elif args.test_type == 'extensive':
        tester.run_extensive_test(tickers)
    
    test_elapsed = time.time() - test_start
    logger.info(f"\nTest completed in {test_elapsed:.1f}s")
    
    # Print summary
    tester.print_summary()
    
    # Save results to file
    results_file = LOG_DIRECTORY / f"yfinance_capacity_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.info(f"\nDetailed results saved to: {results_file}")


if __name__ == "__main__":
    main()
