"""Data validation and quality checks for exchange rate data."""
from typing import List, Dict, Any, Tuple
from datetime import datetime, date
from src.utils.logger import get_logger

logger = get_logger("transform.data_validator")


class DataValidator:
    """Handles data validation and quality checks."""
    
    # Expected schema
    REQUIRED_FIELDS = ['date', 'currency_pair', 'rate', 'base_currency', 'target_currency']
    FIELD_TYPES = {
        'date': str,
        'currency_pair': str,
        'rate': (int, float),
        'base_currency': str,
        'target_currency': str
    }
    
    # Business rules
    MIN_RATE = 0.0001  # Minimum reasonable exchange rate
    MAX_RATE = 1000000  # Maximum reasonable exchange rate
    VALID_CURRENCIES = ['USD', 'EUR', 'GBP', 'GHS']
    
    @classmethod
    def validate_schema(cls, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate data schema (required fields and types).
        
        Args:
            data: Data records to validate
            
        Returns:
            Tuple of (valid_records, error_messages)
        """
        valid_records = []
        errors = []
        
        for idx, record in enumerate(data):
            record_errors = []
            
            # Check required fields
            for field in cls.REQUIRED_FIELDS:
                if field not in record or record[field] is None:
                    record_errors.append(f"Missing required field: {field}")
            
            # Check field types
            for field, expected_type in cls.FIELD_TYPES.items():
                if field in record and record[field] is not None:
                    if not isinstance(record[field], expected_type):
                        record_errors.append(
                            f"Field {field} has wrong type: "
                            f"expected {expected_type}, got {type(record[field])}"
                        )
            
            if record_errors:
                errors.append(f"Record {idx}: {'; '.join(record_errors)}")
                logger.warning(f"Schema validation failed for record {idx}: {record_errors}")
            else:
                valid_records.append(record)
        
        if errors:
            logger.info(f"Schema validation: {len(valid_records)}/{len(data)} records passed")
        
        return valid_records, errors
    
    @classmethod
    def validate_business_rules(cls, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate business rules (rate ranges, currency codes, dates).
        
        Args:
            data: Data records to validate
            
        Returns:
            Tuple of (valid_records, error_messages)
        """
        valid_records = []
        errors = []
        
        for idx, record in enumerate(data):
            record_errors = []
            
            # Validate rate range
            rate = record.get('rate')
            if rate is not None:
                if not (cls.MIN_RATE <= rate <= cls.MAX_RATE):
                    record_errors.append(
                        f"Rate {rate} outside valid range [{cls.MIN_RATE}, {cls.MAX_RATE}]"
                    )
            
            # Validate currency codes
            base_currency = record.get('base_currency', '').upper()
            target_currency = record.get('target_currency', '').upper()
            
            if base_currency not in cls.VALID_CURRENCIES:
                record_errors.append(f"Invalid base currency: {base_currency}")
            
            if target_currency not in cls.VALID_CURRENCIES:
                record_errors.append(f"Invalid target currency: {target_currency}")
            
            # Validate date format and range
            date_str = record.get('date')
            if date_str:
                try:
                    record_date = datetime.fromisoformat(date_str).date()
                    today = date.today()
                    
                    # Date should not be in the future
                    if record_date > today:
                        record_errors.append(f"Date {date_str} is in the future")
                    
                    # Date should not be too old (e.g., more than 10 years)
                    years_ago = (today - record_date).days / 365.25
                    if years_ago > 10:
                        record_errors.append(f"Date {date_str} is more than 10 years old")
                        
                except ValueError:
                    record_errors.append(f"Invalid date format: {date_str}")
            
            if record_errors:
                errors.append(f"Record {idx}: {'; '.join(record_errors)}")
                logger.warning(f"Business rule validation failed for record {idx}: {record_errors}")
            else:
                valid_records.append(record)
        
        if errors:
            logger.info(f"Business rule validation: {len(valid_records)}/{len(data)} records passed")
        
        return valid_records, errors
    
    @classmethod
    def validate(cls, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Perform all validation checks and return quality metrics.
        
        Args:
            data: Data records to validate
            
        Returns:
            Tuple of (valid_records, quality_metrics)
        """
        initial_count = len(data)
        
        # Schema validation
        schema_valid, schema_errors = cls.validate_schema(data)
        
        # Business rule validation
        business_valid, business_errors = cls.validate_business_rules(schema_valid)
        
        # Calculate quality metrics
        quality_metrics = {
            'total_records': initial_count,
            'valid_records': len(business_valid),
            'invalid_records': initial_count - len(business_valid),
            'schema_errors': len(schema_errors),
            'business_rule_errors': len(business_errors),
            'completeness': len(business_valid) / initial_count if initial_count > 0 else 0,
            'all_errors': schema_errors + business_errors
        }
        
        logger.info(
            f"Validation complete: {quality_metrics['valid_records']}/{quality_metrics['total_records']} "
            f"records passed (completeness: {quality_metrics['completeness']:.2%})"
        )
        
        return business_valid, quality_metrics

