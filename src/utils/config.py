"""Configuration management for the ETL pipeline."""
import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()


class Config:
    """Centralized configuration management."""
    
    # ExchangeRate.host API Configuration
    EXCHANGERATE_API_KEY: str = os.getenv("EXCHANGERATE_API_KEY", "cbf4d09c53b4d9a283362efa1895d665")
    EXCHANGERATE_BASE_URL: str = os.getenv("EXCHANGERATE_BASE_URL", "https://api.exchangerate.host")
    
    # Supabase Configuration
    SUPABASE_URL: Optional[str] = os.getenv("SUPABASE_URL")
    SUPABASE_KEY: Optional[str] = os.getenv("SUPABASE_KEY")
    
    # Pipeline Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    
    # Currency pairs to fetch
    BASE_CURRENCIES: list = ["USD", "EUR", "GBP"]
    TARGET_CURRENCY: str = "GHS"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that required configuration is present."""
        if not cls.EXCHANGERATE_API_KEY:
            raise ValueError("EXCHANGERATE_API_KEY is required")
        return True

