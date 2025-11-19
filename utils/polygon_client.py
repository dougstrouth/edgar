# -*- coding: utf-8 -*-
"""
Massive.com (formerly Polygon.io) API Client Utility

Provides a clean interface for fetching stock data from Massive.com with:
- Automatic rate limiting (5 calls/minute for free tier)
- Retry logic with exponential backoff
- Error handling and logging
- Support for v2 aggregates (OHLCV data) and v3 reference data (tickers, details)

Note: Polygon.io rebranded as Massive.com on Oct 30, 2025.
Existing API keys continue to work. The API now defaults to api.massive.com,
while api.polygon.io remains supported for backward compatibility.

Free tier limits:
- 5 API calls per minute
- Previous day's data (1 day delay)
- Unlimited historical data

API Versions:
- v2: Aggregates (OHLCV bars) - /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
- v3: Reference data (tickers, details) - /v3/reference/tickers
"""

import time
import logging
from datetime import date
from typing import Optional, Dict, Any, List
import requests  # type: ignore

logger = logging.getLogger(__name__)


class PolygonRateLimiter:
    """
    Rate limiter for Massive.com (formerly Polygon.io) API.
    Free tier: 5 calls per minute.
    """
    
    def __init__(self, calls_per_minute: int = 5):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # seconds between calls
        self.last_call_time: Optional[float] = None
        self.call_count = 0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        if self.last_call_time is not None:
            elapsed = time.time() - self.last_call_time
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                # Add a small random jitter to avoid sync storms
                jitter = min(1.0, wait_time * 0.1)
                time.sleep(wait_time + (jitter * (0.5 - time.time() % 1)))
        
        self.last_call_time = time.time()
        self.call_count += 1


class PolygonClient:
    """
    Client for Massive.com (formerly Polygon.io) REST API.
    
    Note: Polygon.io rebranded as Massive.com on Oct 30, 2025.
    The new API endpoint is api.massive.com, but api.polygon.io still works.
    
    Documentation: https://massive.com/docs/stocks/getting-started
    """
    
    BASE_URL = "https://api.massive.com"  # New endpoint (api.polygon.io still works)
    
    def __init__(
        self,
        api_key: str,
        rate_limiter: Optional[PolygonRateLimiter] = None,
        max_retries: int = 3,
        retry_delay: float = 5.0
    ):
        """
        Initialize Massive.com (formerly Polygon.io) client.
        
        Args:
            api_key: Your Massive.com/Polygon.io API key (existing keys still work)
            rate_limiter: Optional rate limiter (default: 5 calls/min)
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay for exponential backoff (seconds)
        """
        self.api_key = api_key
        self.rate_limiter = rate_limiter or PolygonRateLimiter()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a request to Massive.com API with rate limiting and retries.
        
        Args:
            endpoint: API endpoint (e.g., '/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-12-31')
            params: Optional query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.exceptions.RequestException: On API errors
        """
        if params is None:
            params = {}
        
        # Add API key to params
        params['apiKey'] = self.api_key
        
        url = f"{self.BASE_URL}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                # Wait for rate limit
                self.rate_limiter.wait_if_needed()
                
                # Make request
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limit (429) or server errors (5xx)
                if response.status_code == 429:
                    # Honor Retry-After header if present, otherwise use aggressive backoff
                    ra = response.headers.get('Retry-After')
                    try:
                        wait_time = float(ra) if ra is not None else (self.retry_delay * (3 ** attempt))
                    except Exception:
                        wait_time = self.retry_delay * (3 ** attempt)

                    # Add jitter to avoid thundering herd
                    jitter = min(5.0, wait_time * 0.2)
                    sleep_time = wait_time + (jitter * (0.5 - time.time() % 1))
                    logger.warning(f"Rate limit hit (429). Waiting {sleep_time:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(sleep_time)
                    # After a 429, significantly increase internal spacing to prevent future limits
                    try:
                        self.rate_limiter.min_interval = max(self.rate_limiter.min_interval, 15.0)
                    except Exception:
                        pass
                    continue
                    
                if response.status_code >= 500:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Server error ({response.status_code}). Retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                
                # Raise for other HTTP errors
                response.raise_for_status()
                
                # Parse JSON
                data = response.json()
                
                # Check for API-level errors
                if data.get('status') == 'ERROR':
                    error_msg = data.get('error', 'Unknown error')
                    raise requests.exceptions.RequestException(f"Massive.com API error: {error_msg}")
                
                return data
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request timeout. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    raise
                    
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed: {e}. Retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    raise
        
        raise requests.exceptions.RequestException(f"Failed after {self.max_retries} retries")
    
    def get_aggregates(
        self,
        ticker: str,
        from_date: date,
        to_date: date,
        timespan: str = "day",
        multiplier: int = 1
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get aggregate bars (OHLCV) for a ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            from_date: Start date
            to_date: End date
            timespan: Bar timespan ('day', 'week', 'month', 'quarter', 'year')
            multiplier: Timespan multiplier (e.g., 1 for 1-day bars)
            
        Returns:
            List of bar dictionaries with keys: v (volume), vw (vwap), o (open),
            c (close), h (high), l (low), t (timestamp), n (transactions)
            Returns None if no data found
            
        Example:
            bars = client.get_aggregates('AAPL', date(2023, 1, 1), date(2023, 12, 31))
        """
        # Ensure dates are in YYYY-MM-DD format (not datetime with time)
        from_str = from_date.strftime('%Y-%m-%d') if hasattr(from_date, 'strftime') else str(from_date)
        to_str = to_date.strftime('%Y-%m-%d') if hasattr(to_date, 'strftime') else str(to_date)
        
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_str}/{to_str}"
        
        params = {
            'adjusted': 'true',  # Adjust for splits
            'sort': 'asc',       # Chronological order
            'limit': 50000       # Max results (plenty for daily data)
        }
        
        try:
            data = self._make_request(endpoint, params)
            
            # Check if we got results
            results_count = data.get('resultsCount', 0)
            if results_count == 0:
                logger.debug(f"No data found for {ticker} from {from_date} to {to_date}")
                return None
            
            results = data.get('results', [])
            logger.debug(f"Retrieved {len(results)} bars for {ticker}")
            return results
            
        except requests.exceptions.RequestException as e:
            # Re-raise exception instead of returning None
            # This ensures fetch_worker properly counts it as an error
            logger.error(f"Failed to fetch aggregates for {ticker}: {e}")
            raise
    
    def get_ticker_details(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get details/info for a ticker using v3 API (company name, market, sector, etc).
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with ticker details including: name, market, locale, 
            primary_exchange, type, active status, cik, currency, etc.
            Returns None if not found
        """
        endpoint = f"/v3/reference/tickers/{ticker}"
        
        try:
            data = self._make_request(endpoint)
            return data.get('results')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch details for {ticker}: {e}")
            return None
    
    def get_all_tickers(
        self, 
        active: bool = True,
        market: str = "stocks",
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get all available tickers using v3 API with pagination support.
        
        Args:
            active: Only return actively traded tickers (default: True)
            market: Market type - 'stocks', 'crypto', 'fx', 'otc', 'indices' (default: stocks)
            limit: Results per page (max 1000, default 1000)
            
        Returns:
            List of ticker dictionaries with details (ticker, name, market, active, cik, etc)
            
        Example:
            all_stocks = client.get_all_tickers(active=True, market='stocks')
        """
        endpoint = "/v3/reference/tickers"
        params = {
            'active': str(active).lower(),
            'market': market,
            'limit': limit,
            'order': 'asc',
            'sort': 'ticker'
        }
        
        all_results = []
        page = 1
        try:
            while True:
                # Apply rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Make request
                response = self.session.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                if not results:
                    break
                    
                all_results.extend(results)
                logger.info(f"Page {page}: fetched {len(results)} tickers (total: {len(all_results)})")
                
                # Check for next page using next_url
                next_url = data.get('next_url')
                if not next_url:
                    break
                
                # Parse cursor from next_url and update params
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(next_url)
                query_params = parse_qs(parsed.query)
                cursor = query_params.get('cursor', [None])[0]
                
                if not cursor:
                    break
                    
                params['cursor'] = cursor
                page += 1
                
            logger.info(f"Retrieved {len(all_results)} total tickers across {page} pages")
            return all_results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch tickers: {e}")
            return all_results  # Return what we got so far
    
    def is_ticker_valid(self, ticker: str) -> bool:
        """
        Check if a ticker is valid and actively traded using v3 API.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            True if ticker exists and is active, False otherwise
        """
        details = self.get_ticker_details(ticker)
        if not details:
            return False
        return details.get('active', False)
    
    @staticmethod
    def format_ticker_for_db(ticker_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format v3 ticker API response for database storage.
        
        Args:
            ticker_data: Raw ticker data from v3 API
            
        Returns:
            Dictionary with keys matching the tickers table schema
            
        Example:
            data = client.get_ticker_details('AAPL')
            formatted = PolygonClient.format_ticker_for_db(data)
        """
        from datetime import datetime
        
        def parse_timestamp(ts_str):
            """Parse UTC timestamp string to datetime."""
            if not ts_str:
                return None
            try:
                return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            except Exception:
                return None
        
        return {
            'ticker': ticker_data.get('ticker'),
            'cik': ticker_data.get('cik'),
            'name': ticker_data.get('name'),
            'market': ticker_data.get('market'),
            'locale': ticker_data.get('locale'),
            'primary_exchange': ticker_data.get('primary_exchange'),
            'type': ticker_data.get('type'),
            'active': ticker_data.get('active'),
            'currency_name': ticker_data.get('currency_name'),
            'currency_symbol': ticker_data.get('currency_symbol'),
            'base_currency_name': ticker_data.get('base_currency_name'),
            'base_currency_symbol': ticker_data.get('base_currency_symbol'),
            'composite_figi': ticker_data.get('composite_figi'),
            'share_class_figi': ticker_data.get('share_class_figi'),
            'last_updated_utc': parse_timestamp(ticker_data.get('last_updated_utc')),
            'delisted_utc': parse_timestamp(ticker_data.get('delisted_utc'))
        }
    
    def get_previous_close(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get previous day's OHLCV for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with previous close data or None if not found
        """
        endpoint = f"/v2/aggs/ticker/{ticker}/prev"
        
        try:
            data = self._make_request(endpoint)
            results = data.get('results', [])
            return results[0] if results else None
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch previous close for {ticker}: {e}")
            return None
    
    def check_connectivity(self) -> bool:
        """
        Test API connectivity and authentication.
        
        Returns:
            True if API is accessible, False otherwise
        """
        try:
            # Try to get previous close for a well-known ticker
            result = self.get_previous_close('AAPL')
            return result is not None
        except Exception as e:
            logger.error(f"Connectivity check failed: {e}")
            return False
