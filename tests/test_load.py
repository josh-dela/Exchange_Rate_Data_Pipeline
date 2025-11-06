"""Tests for Load layer."""
import pytest
from unittest.mock import Mock, MagicMock, patch
from src.load.supabase_loader import SupabaseLoader


class TestSupabaseLoader:
    """Test cases for SupabaseLoader."""
    
    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
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
    
    def test_is_configured_false(self):
        """Test when Supabase is not configured."""
        loader = SupabaseLoader(supabase_url=None, supabase_key=None)
        
        assert loader.is_configured() is False
    
    @patch('src.load.supabase_loader.create_client')
    def test_is_configured_true(self, mock_create_client):
        """Test when Supabase is configured."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        
        loader = SupabaseLoader(supabase_url="https://test.supabase.co", supabase_key="test_key")
        
        assert loader.is_configured() is True
        assert loader.client == mock_client
    
    def test_prepare_data(self, sample_data):
        """Test data preparation."""
        loader = SupabaseLoader(supabase_url=None, supabase_key=None)
        
        prepared = loader._prepare_data(sample_data)
        
        assert len(prepared) == 2
        assert all('date' in r for r in prepared)
        assert all('currency_pair' in r for r in prepared)
        assert all('rate' in r for r in prepared)
        # Should not include extra fields
        assert 'fetched_at' not in prepared[0]
    
    @patch('src.load.supabase_loader.create_client')
    def test_load_batch_not_configured(self, mock_create_client, sample_data):
        """Test load when Supabase is not configured."""
        loader = SupabaseLoader(supabase_url=None, supabase_key=None)
        
        result = loader.load_batch(sample_data)
        
        assert result['skipped'] is True
        assert result['success_count'] == 0
        assert result['error_count'] == len(sample_data)
    
    @patch('src.load.supabase_loader.create_client')
    def test_load_batch_success(self, mock_create_client, sample_data):
        """Test successful batch loading."""
        # Mock Supabase client
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_upsert = MagicMock()
        mock_response = MagicMock()
        mock_response.data = sample_data
        
        mock_upsert.execute.return_value = mock_response
        mock_table.upsert.return_value = mock_upsert
        mock_client.table.return_value = mock_table
        mock_create_client.return_value = mock_client
        
        loader = SupabaseLoader(supabase_url="https://test.supabase.co", supabase_key="test_key")
        
        result = loader.load_batch(sample_data, batch_size=10)
        
        assert result['success_count'] == 2
        assert result['error_count'] == 0
        assert result['skipped'] is False
        mock_table.upsert.assert_called_once()
    
    @patch('src.load.supabase_loader.create_client')
    def test_load_batch_with_error(self, mock_create_client, sample_data):
        """Test load batch with error handling."""
        # Mock Supabase client that raises error
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.upsert.side_effect = Exception("Database error")
        mock_client.table.return_value = mock_table
        mock_create_client.return_value = mock_client
        
        loader = SupabaseLoader(supabase_url="https://test.supabase.co", supabase_key="test_key")
        
        result = loader.load_batch(sample_data)
        
        assert result['error_count'] == 2
        assert len(result['errors']) > 0
    
    @patch('src.load.supabase_loader.create_client')
    def test_test_connection_success(self, mock_create_client):
        """Test successful connection test."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_select = MagicMock()
        mock_limit = MagicMock()
        mock_response = MagicMock()
        mock_response.data = []
        
        mock_limit.execute.return_value = mock_response
        mock_select.limit.return_value = mock_limit
        mock_table.select.return_value = mock_select
        mock_client.table.return_value = mock_table
        mock_create_client.return_value = mock_client
        
        loader = SupabaseLoader(supabase_url="https://test.supabase.co", supabase_key="test_key")
        
        assert loader.test_connection() is True
    
    @patch('src.load.supabase_loader.create_client')
    def test_test_connection_failure(self, mock_create_client):
        """Test connection test failure."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.select.side_effect = Exception("Connection failed")
        mock_client.table.return_value = mock_table
        mock_create_client.return_value = mock_client
        
        loader = SupabaseLoader(supabase_url="https://test.supabase.co", supabase_key="test_key")
        
        assert loader.test_connection() is False

