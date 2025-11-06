"""Data cleaning operations for exchange rate data."""
from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("transform.data_cleaner")


class DataCleaner:
    """Handles data cleaning operations."""
    
    @staticmethod
    def clean_exchange_rate_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean exchange rate data by removing duplicates, handling missing values, etc.
        
        Args:
            data: Raw exchange rate data from extract layer
            
        Returns:
            Cleaned data list
        """
        if not data:
            logger.warning("No data provided for cleaning")
            return []
        
        logger.info(f"Cleaning {len(data)} records")
        
        # Remove duplicates based on date and currency_pair
        cleaned_data = DataCleaner._remove_duplicates(data)
        
        # Handle missing values
        cleaned_data = DataCleaner._handle_missing_values(cleaned_data)
        
        # Standardize currency pair formats
        cleaned_data = DataCleaner._standardize_currency_pairs(cleaned_data)
        
        # Type conversion and validation
        cleaned_data = DataCleaner._convert_types(cleaned_data)
        
        logger.info(f"Cleaned data: {len(cleaned_data)} records remaining")
        return cleaned_data
    
    @staticmethod
    def _remove_duplicates(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate records based on date and currency_pair."""
        seen = set()
        unique_data = []
        duplicates_count = 0
        
        for record in data:
            key = (record.get('date'), record.get('currency_pair'))
            if key not in seen:
                seen.add(key)
                unique_data.append(record)
            else:
                duplicates_count += 1
        
        if duplicates_count > 0:
            logger.info(f"Removed {duplicates_count} duplicate records")
        
        return unique_data
    
    @staticmethod
    def _handle_missing_values(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove records with missing critical fields."""
        required_fields = ['date', 'currency_pair', 'rate', 'base_currency', 'target_currency']
        cleaned_data = []
        removed_count = 0
        
        for record in data:
            if all(record.get(field) is not None for field in required_fields):
                cleaned_data.append(record)
            else:
                removed_count += 1
                logger.warning(f"Removed record with missing values: {record}")
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} records with missing values")
        
        return cleaned_data
    
    @staticmethod
    def _standardize_currency_pairs(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Standardize currency pair format (e.g., 'USD/GHS' or 'USD-GHS' -> 'USD/GHS')."""
        for record in data:
            base = record.get('base_currency', '').upper().strip()
            target = record.get('target_currency', '').upper().strip()
            
            if base and target:
                record['base_currency'] = base
                record['target_currency'] = target
                record['currency_pair'] = f"{base}/{target}"
        
        return data
    
    @staticmethod
    def _convert_types(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert data types to ensure consistency."""
        for record in data:
            # Ensure rate is float
            if 'rate' in record:
                try:
                    record['rate'] = float(record['rate'])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid rate value: {record.get('rate')}")
                    record['rate'] = None
            
            # Ensure date is string in ISO format
            if 'date' in record:
                date_val = record['date']
                if isinstance(date_val, datetime):
                    record['date'] = date_val.date().isoformat()
                elif isinstance(date_val, str):
                    # Validate date format
                    try:
                        datetime.fromisoformat(date_val)
                    except ValueError:
                        logger.warning(f"Invalid date format: {date_val}")
        
        # Remove records with invalid rates
        cleaned_data = [r for r in data if r.get('rate') is not None]
        
        return cleaned_data

