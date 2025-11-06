"""Supabase data loader for storing exchange rate data."""
from typing import List, Dict, Any, Optional
from supabase import create_client, Client
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger("load.supabase_loader")


class SupabaseLoader:
    """Handles loading data into Supabase."""
    
    def __init__(self, supabase_url: Optional[str] = None, supabase_key: Optional[str] = None):
        """Initialize Supabase client.
        
        Args:
            supabase_url: Supabase project URL
            supabase_key: Supabase API key
        """
        self.supabase_url = supabase_url or Config.SUPABASE_URL
        self.supabase_key = supabase_key or Config.SUPABASE_KEY
        self.client: Optional[Client] = None
        self.table_name = "exchange_rates"
        
        if self.supabase_url and self.supabase_key:
            try:
                self.client = create_client(self.supabase_url, self.supabase_key)
                logger.info("Supabase client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Supabase client: {e}")
                raise
        else:
            logger.warning("Supabase credentials not provided. Load operations will be skipped.")
    
    def is_configured(self) -> bool:
        """Check if Supabase is properly configured."""
        return self.client is not None
    
    def _prepare_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare data for insertion by selecting only required fields.
        
        Args:
            data: Raw data records
            
        Returns:
            Prepared data with only database fields
        """
        prepared = []
        for record in data:
            prepared.append({
                'date': record.get('date'),
                'currency_pair': record.get('currency_pair'),
                'rate': record.get('rate'),
                'base_currency': record.get('base_currency'),
                'target_currency': record.get('target_currency')
            })
        return prepared
    
    def load_batch(self, data: List[Dict[str, Any]], batch_size: Optional[int] = None) -> Dict[str, Any]:
        """Load data in batches with upsert logic to handle duplicates.
        
        Args:
            data: Data records to load
            batch_size: Number of records per batch (default: Config.BATCH_SIZE)
            
        Returns:
            Dictionary with load results (success_count, error_count, errors)
        """
        if not self.is_configured():
            logger.warning("Supabase not configured. Skipping load operation.")
            return {
                'success_count': 0,
                'error_count': len(data),
                'errors': ['Supabase not configured'],
                'skipped': True
            }
        
        if not data:
            logger.warning("No data provided for loading")
            return {'success_count': 0, 'error_count': 0, 'errors': []}
        
        batch_size = batch_size or Config.BATCH_SIZE
        prepared_data = self._prepare_data(data)
        
        logger.info(f"Loading {len(prepared_data)} records in batches of {batch_size}")
        
        success_count = 0
        error_count = 0
        errors = []
        
        # Process in batches
        for i in range(0, len(prepared_data), batch_size):
            batch = prepared_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(prepared_data) + batch_size - 1) // batch_size
            
            try:
                logger.debug(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                # Use upsert to handle duplicates (based on date + currency_pair unique constraint)
                response = self.client.table(self.table_name).upsert(
                    batch,
                    on_conflict="date,currency_pair"
                ).execute()
                
                batch_success = len(batch)
                success_count += batch_success
                logger.info(f"Batch {batch_num}/{total_batches} loaded successfully: {batch_success} records")
                
            except Exception as e:
                error_msg = f"Error loading batch {batch_num}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                error_count += len(batch)
        
        result = {
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors,
            'total_records': len(prepared_data)
        }
        
        logger.info(
            f"Load complete: {success_count} successful, {error_count} failed "
            f"out of {len(prepared_data)} total records"
        )
        
        return result
    
    def test_connection(self) -> bool:
        """Test Supabase connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        if not self.is_configured():
            return False
        
        try:
            # Try a simple query to test connection
            response = self.client.table(self.table_name).select("id").limit(1).execute()
            logger.info("Supabase connection test successful")
            return True
        except Exception as e:
            logger.error(f"Supabase connection test failed: {e}")
            return False

