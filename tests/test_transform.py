"""Tests for Transform layer."""
import pytest
from datetime import date, datetime
from src.transform.data_cleaner import DataCleaner
from src.transform.data_validator import DataValidator


class TestDataCleaner:
    """Test cases for DataCleaner."""
    
    @pytest.fixture
    def sample_data(self):
        """Sample exchange rate data for testing."""
        return [
            {
                'date': '2024-01-15',
                'base_currency': 'USD',
                'target_currency': 'GHS',
                'rate': 12.5,
                'currency_pair': 'USD/GHS'
            },
            {
                'date': '2024-01-15',
                'base_currency': 'EUR',
                'target_currency': 'GHS',
                'rate': 13.5,
                'currency_pair': 'EUR/GHS'
            }
        ]
    
    def test_remove_duplicates(self, sample_data):
        """Test duplicate removal."""
        # Add duplicate
        data_with_duplicates = sample_data + [sample_data[0]]
        
        cleaned = DataCleaner._remove_duplicates(data_with_duplicates)
        
        assert len(cleaned) == 2
        assert len([r for r in cleaned if r['currency_pair'] == 'USD/GHS']) == 1
    
    def test_handle_missing_values(self, sample_data):
        """Test handling of missing values."""
        # Add record with missing rate
        data_with_missing = sample_data + [
            {
                'date': '2024-01-16',
                'base_currency': 'GBP',
                'target_currency': 'GHS',
                'rate': None,
                'currency_pair': 'GBP/GHS'
            }
        ]
        
        cleaned = DataCleaner._handle_missing_values(data_with_missing)
        
        assert len(cleaned) == 2
        assert all(r.get('rate') is not None for r in cleaned)
    
    def test_standardize_currency_pairs(self):
        """Test currency pair standardization."""
        data = [
            {
                'base_currency': 'usd',
                'target_currency': 'ghs',
                'currency_pair': 'USD-GHS'
            }
        ]
        
        cleaned = DataCleaner._standardize_currency_pairs(data)
        
        assert cleaned[0]['base_currency'] == 'USD'
        assert cleaned[0]['target_currency'] == 'GHS'
        assert cleaned[0]['currency_pair'] == 'USD/GHS'
    
    def test_convert_types(self):
        """Test type conversion."""
        data = [
            {
                'date': datetime(2024, 1, 15),
                'rate': '12.5',
                'base_currency': 'USD',
                'target_currency': 'GHS',
                'currency_pair': 'USD/GHS'
            }
        ]
        
        cleaned = DataCleaner._convert_types(data)
        
        assert isinstance(cleaned[0]['rate'], float)
        assert cleaned[0]['rate'] == 12.5
        assert isinstance(cleaned[0]['date'], str)
    
    def test_clean_exchange_rate_data(self, sample_data):
        """Test complete cleaning process."""
        # Add some problematic records
        messy_data = sample_data + [
            sample_data[0],  # Duplicate
            {'date': '2024-01-16', 'rate': None},  # Missing values
            {'date': '2024-01-17', 'rate': 'invalid'}  # Invalid rate
        ]
        
        cleaned = DataCleaner.clean_exchange_rate_data(messy_data)
        
        assert len(cleaned) == 2
        assert all(isinstance(r['rate'], float) for r in cleaned)


class TestDataValidator:
    """Test cases for DataValidator."""
    
    @pytest.fixture
    def valid_data(self):
        """Valid exchange rate data."""
        return [
            {
                'date': '2024-01-15',
                'base_currency': 'USD',
                'target_currency': 'GHS',
                'rate': 12.5,
                'currency_pair': 'USD/GHS'
            }
        ]
    
    def test_validate_schema_success(self, valid_data):
        """Test successful schema validation."""
        valid, errors = DataValidator.validate_schema(valid_data)
        
        assert len(valid) == 1
        assert len(errors) == 0
    
    def test_validate_schema_missing_field(self, valid_data):
        """Test schema validation with missing field."""
        invalid_data = [{'date': '2024-01-15'}]  # Missing required fields
        
        valid, errors = DataValidator.validate_schema(invalid_data)
        
        assert len(valid) == 0
        assert len(errors) > 0
    
    def test_validate_schema_wrong_type(self, valid_data):
        """Test schema validation with wrong type."""
        invalid_data = [{**valid_data[0], 'rate': 'not_a_number'}]
        
        valid, errors = DataValidator.validate_schema(invalid_data)
        
        assert len(valid) == 0
        assert len(errors) > 0
    
    def test_validate_business_rules_rate_range(self, valid_data):
        """Test business rule validation for rate range."""
        # Rate too high
        invalid_data = [{**valid_data[0], 'rate': 2000000}]
        
        valid, errors = DataValidator.validate_business_rules(invalid_data)
        
        assert len(valid) == 0
        assert len(errors) > 0
    
    def test_validate_business_rules_invalid_currency(self, valid_data):
        """Test business rule validation for currency codes."""
        invalid_data = [{**valid_data[0], 'base_currency': 'XXX'}]
        
        valid, errors = DataValidator.validate_business_rules(invalid_data)
        
        assert len(valid) == 0
        assert len(errors) > 0
    
    def test_validate_business_rules_future_date(self, valid_data):
        """Test business rule validation for future dates."""
        from datetime import date, timedelta
        future_date = (date.today() + timedelta(days=1)).isoformat()
        invalid_data = [{**valid_data[0], 'date': future_date}]
        
        valid, errors = DataValidator.validate_business_rules(invalid_data)
        
        assert len(valid) == 0
        assert len(errors) > 0
    
    def test_validate_complete(self, valid_data):
        """Test complete validation process."""
        # Mix of valid and invalid records
        mixed_data = valid_data + [
            {'date': '2024-01-16', 'rate': None},  # Missing rate
            {**valid_data[0], 'rate': 2000000}  # Invalid rate
        ]
        
        valid, metrics = DataValidator.validate(mixed_data)
        
        assert len(valid) == 1
        assert metrics['total_records'] == 3
        assert metrics['valid_records'] == 1
        assert metrics['completeness'] < 1.0

