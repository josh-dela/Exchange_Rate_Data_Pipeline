"""Tests for Extract layer."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from src.extract.api_client import ExchangeRateAPIClient


class TestExchangeRateAPIClient:
    """Test cases for ExchangeRateAPIClient."""
    
    @pytest.fixture
    def api_client(self):
        """Create API client instance for testing."""
        return ExchangeRateAPIClient(api_key="test_key", base_url="https://api.test.com")
    
    @patch('src.extract.api_client.requests.get')
    def test_fetch_latest_rates_success(self, mock_get, api_client):
        """Test successful rate fetching."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            'success': True,
            'rates': {
                'GHS': 12.5,
                'EUR': 0.92,
                'GBP': 0.79
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Test fetching rates
        result = api_client.fetch_latest_rates(['USD', 'EUR', 'GBP'], 'GHS')
        
        # Assertions
        assert len(result) == 3
        assert all('rate' in r for r in result)
        assert all('currency_pair' in r for r in result)
        assert any(r['base_currency'] == 'USD' for r in result)
        assert any(r['base_currency'] == 'EUR' for r in result)
        assert any(r['base_currency'] == 'GBP' for r in result)
    
    @patch('src.extract.api_client.requests.get')
    def test_fetch_latest_rates_api_error(self, mock_get, api_client):
        """Test handling of API errors."""
        # Mock API error response
        mock_response = Mock()
        mock_response.json.return_value = {
            'success': False,
            'error': {'info': 'Invalid API key'}
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Test error handling
        with pytest.raises(ValueError, match="API error"):
            api_client.fetch_latest_rates()
    
    @patch('src.extract.api_client.requests.get')
    def test_fetch_latest_rates_retry(self, mock_get, api_client):
        """Test retry logic on request failure."""
        # Mock first two failures, then success
        mock_response = Mock()
        mock_response.json.return_value = {
            'success': True,
            'rates': {'GHS': 12.5, 'EUR': 0.92, 'GBP': 0.79}
        }
        mock_response.raise_for_status = Mock()
        
        # First two calls fail, third succeeds
        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Network error"),
            mock_response
        ]
        
        result = api_client.fetch_latest_rates(['USD'], 'GHS')
        
        assert len(result) > 0
        assert mock_get.call_count == 3
    
    @patch('src.extract.api_client.requests.get')
    def test_fetch_historical_rate(self, mock_get, api_client):
        """Test fetching historical rates."""
        test_date = date(2024, 1, 15)
        
        mock_response = Mock()
        mock_response.json.return_value = {
            'success': True,
            'rates': {'GHS': 12.3}
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result = api_client.fetch_historical_rate(test_date, 'USD', 'GHS')
        
        assert result is not None
        assert result['date'] == test_date.isoformat()
        assert result['rate'] == 12.3
        assert result['currency_pair'] == 'USD/GHS'

