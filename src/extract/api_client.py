"""API client for fetching exchange rates from ExchangeRate.host."""
import requests
from typing import Dict, List, Optional
from datetime import datetime, date
from time import sleep
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger("extract.api_client")


class ExchangeRateAPIClient:
    """Client for interacting with ExchangeRate.host API."""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """Initialize the API client.
        
        Args:
            api_key: API key for ExchangeRate.host. Defaults to Config value.
            base_url: Base URL for the API. Defaults to Config value.
        """
        self.api_key = api_key or Config.EXCHANGERATE_API_KEY
        self.base_url = base_url or Config.EXCHANGERATE_BASE_URL
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make HTTP request with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.RequestException: If all retries fail
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params['access_key'] = self.api_key
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"API request attempt {attempt + 1}: {url}")
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Check for API errors in response
                if not data.get('success', True):
                    error_msg = data.get('error', {}).get('info', 'Unknown API error')
                    raise ValueError(f"API error: {error_msg}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying...")
                    sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"All retry attempts failed: {e}")
                    raise
        
    def fetch_latest_rates(self, base_currencies: List[str] = None, 
                          target_currency: str = None) -> List[Dict]:
        """Fetch latest exchange rates for specified currency pairs.
        
        Args:
            base_currencies: List of base currencies (default: USD, EUR, GBP)
            target_currency: Target currency (default: GHS)
            
        Returns:
            List of dictionaries with rate data:
            [
                {
                    'date': '2024-01-15',
                    'base_currency': 'USD',
                    'target_currency': 'GHS',
                    'rate': 12.3456,
                    'currency_pair': 'USD/GHS'
                },
                ...
            ]
        """
        base_currencies = base_currencies or Config.BASE_CURRENCIES
        target_currency = target_currency or Config.TARGET_CURRENCY
        
        logger.info(f"Fetching latest rates: {base_currencies} -> {target_currency}")
        
        # Fetch all rates at once using USD as base
        symbols = ",".join([target_currency] + [c for c in base_currencies if c != "USD"])
        
        try:
            data = self._make_request("latest", {
                "base": "USD",
                "symbols": symbols
            })
            
            rates = data.get('rates', {})
            if not rates:
                raise ValueError("No rates returned from API")
            
            # Extract rates and convert to target currency
            result = []
            fetch_date = datetime.now().date()
            
            for base_currency in base_currencies:
                if base_currency == "USD":
                    # Direct rate
                    rate = rates.get(target_currency)
                else:
                    # Convert via USD: target_rate = (USD/target) / (USD/base)
                    usd_to_target = rates.get(target_currency)
                    usd_to_base = rates.get(base_currency)
                    
                    if not usd_to_target or not usd_to_base:
                        logger.warning(f"Missing rate for {base_currency} -> {target_currency}")
                        continue
                    
                    rate = usd_to_target / usd_to_base
                
                if rate:
                    result.append({
                        'date': fetch_date.isoformat(),
                        'base_currency': base_currency,
                        'target_currency': target_currency,
                        'rate': float(rate),
                        'currency_pair': f"{base_currency}/{target_currency}",
                        'fetched_at': datetime.now().isoformat()
                    })
            
            logger.info(f"Successfully fetched {len(result)} exchange rates")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching exchange rates: {e}")
            raise
    
    def fetch_historical_rate(self, date: date, base_currency: str, 
                             target_currency: str) -> Optional[Dict]:
        """Fetch historical exchange rate for a specific date.
        
        Args:
            date: Date to fetch rate for
            base_currency: Base currency code
            target_currency: Target currency code
            
        Returns:
            Dictionary with rate data or None if not found
        """
        logger.debug(f"Fetching historical rate for {base_currency}/{target_currency} on {date}")
        
        try:
            data = self._make_request(f"historical/{date.isoformat()}", {
                "base": base_currency,
                "symbols": target_currency
            })
            
            rates = data.get('rates', {})
            rate = rates.get(target_currency)
            
            if rate:
                return {
                    'date': date.isoformat(),
                    'base_currency': base_currency,
                    'target_currency': target_currency,
                    'rate': float(rate),
                    'currency_pair': f"{base_currency}/{target_currency}",
                    'fetched_at': datetime.now().isoformat()
                }
            return None
            
        except Exception as e:
            logger.error(f"Error fetching historical rate: {e}")
            return None

