"""Generate dummy data for dashboard testing."""
import random
from datetime import datetime, timedelta, date
from typing import List, Dict
from src.utils.config import Config


def generate_dummy_data(days: int = 30, currencies: List[str] = None) -> List[Dict]:
    """Generate realistic dummy exchange rate data.
    
    Args:
        days: Number of days of historical data to generate
        currencies: List of base currencies (default: USD, EUR, GBP)
        
    Returns:
        List of exchange rate records
    """
    currencies = currencies or Config.BASE_CURRENCIES
    target = Config.TARGET_CURRENCY
    
    # Base rates (approximate real values)
    base_rates = {
        'USD': 12.0,
        'EUR': 13.0,
        'GBP': 15.0
    }
    
    data = []
    end_date = date.today()
    
    for i in range(days):
        current_date = end_date - timedelta(days=i)
        
        for currency in currencies:
            # Generate realistic rate with small daily variations
            base_rate = base_rates.get(currency, 12.0)
            
            # Add random walk with slight trend
            variation = random.uniform(-0.05, 0.05)  # ±5% daily variation
            trend = random.uniform(-0.001, 0.001) * i  # Small cumulative trend
            
            rate = base_rate * (1 + variation + trend)
            rate = round(rate, 4)
            
            data.append({
                'date': current_date.isoformat(),
                'base_currency': currency,
                'target_currency': target,
                'rate': rate,
                'currency_pair': f"{currency}/{target}",
                'created_at': datetime.now().isoformat()
            })
    
    # Sort by date (oldest first)
    data.sort(key=lambda x: x['date'])
    
    return data


def generate_dummy_dataframe(days: int = 30, currencies: List[str] = None):
    """Generate dummy data as pandas DataFrame.
    
    Args:
        days: Number of days of historical data to generate
        currencies: List of base currencies
        
    Returns:
        pandas DataFrame with exchange rate data
    """
    try:
        import pandas as pd
        data = generate_dummy_data(days, currencies)
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except ImportError:
        raise ImportError("pandas is required for DataFrame generation")

