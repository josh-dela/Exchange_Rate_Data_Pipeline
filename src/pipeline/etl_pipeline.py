"""Main ETL pipeline orchestrator."""
from typing import Dict, Any, Optional
from datetime import datetime
from src.extract.api_client import ExchangeRateAPIClient
from src.transform.data_cleaner import DataCleaner
from src.transform.data_validator import DataValidator
from src.load.supabase_loader import SupabaseLoader
from src.utils.logger import get_logger, PipelineLogger
from src.utils.config import Config

logger = get_logger("pipeline.etl_pipeline")


class ETLPipeline:
    """Orchestrates the ETL process: Extract, Transform, Load."""
    
    def __init__(self, 
                 api_client: Optional[ExchangeRateAPIClient] = None,
                 supabase_loader: Optional[SupabaseLoader] = None):
        """Initialize ETL pipeline with components.
        
        Args:
            api_client: ExchangeRate API client (default: new instance)
            supabase_loader: Supabase loader (default: new instance)
        """
        self.api_client = api_client or ExchangeRateAPIClient()
        self.data_cleaner = DataCleaner()
        self.data_validator = DataValidator()
        self.supabase_loader = supabase_loader or SupabaseLoader()
        
    def run(self) -> Dict[str, Any]:
        """Execute the complete ETL pipeline.
        
        Returns:
            Dictionary with pipeline execution results and metrics
        """
        start_time = datetime.now()
        PipelineLogger.log_etl_stage("PIPELINE", "Starting ETL pipeline execution")
        
        results = {
            'start_time': start_time.isoformat(),
            'stages': {},
            'success': False,
            'error': None
        }
        
        try:
            # EXTRACT stage
            extract_result = self._extract()
            results['stages']['extract'] = extract_result
            
            if not extract_result['success']:
                raise Exception(f"Extract stage failed: {extract_result.get('error')}")
            
            raw_data = extract_result['data']
            
            # TRANSFORM stage
            transform_result = self._transform(raw_data)
            results['stages']['transform'] = transform_result
            
            if not transform_result['success']:
                raise Exception(f"Transform stage failed: {transform_result.get('error')}")
            
            cleaned_data = transform_result['data']
            quality_metrics = transform_result['quality_metrics']
            
            # LOAD stage
            load_result = self._load(cleaned_data)
            results['stages']['load'] = load_result
            
            if not load_result['success']:
                raise Exception(f"Load stage failed: {load_result.get('error')}")
            
            # Pipeline successful
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                'success': True,
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'records_processed': len(cleaned_data),
                'quality_metrics': quality_metrics,
                'load_metrics': load_result.get('metrics', {})
            })
            
            PipelineLogger.log_etl_stage(
                "PIPELINE",
                "ETL pipeline completed successfully",
                records=len(cleaned_data),
                duration_seconds=duration
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                'success': False,
                'error': str(e),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration
            })
            
            logger.error(f"ETL pipeline failed: {e}")
            PipelineLogger.log_etl_stage("PIPELINE", f"ETL pipeline failed: {e}")
        
        return results
    
    def _extract(self) -> Dict[str, Any]:
        """Execute Extract stage.
        
        Returns:
            Dictionary with extract results
        """
        PipelineLogger.log_etl_stage("EXTRACT", "Starting data extraction")
        
        try:
            data = self.api_client.fetch_latest_rates()
            
            result = {
                'success': True,
                'records_count': len(data),
                'data': data
            }
            
            PipelineLogger.log_etl_stage(
                "EXTRACT",
                "Data extraction completed",
                records=len(data)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Extract stage error: {e}")
            return {
                'success': False,
                'error': str(e),
                'records_count': 0,
                'data': []
            }
    
    def _transform(self, data: list) -> Dict[str, Any]:
        """Execute Transform stage (clean + validate).
        
        Args:
            data: Raw data from extract stage
            
        Returns:
            Dictionary with transform results
        """
        PipelineLogger.log_etl_stage("TRANSFORM", "Starting data transformation")
        
        try:
            # Clean data
            cleaned_data = self.data_cleaner.clean_exchange_rate_data(data)
            
            # Validate data
            valid_data, quality_metrics = self.data_validator.validate(cleaned_data)
            
            result = {
                'success': True,
                'records_count': len(valid_data),
                'data': valid_data,
                'quality_metrics': quality_metrics
            }
            
            PipelineLogger.log_etl_stage(
                "TRANSFORM",
                "Data transformation completed",
                records=len(valid_data),
                completeness=f"{quality_metrics['completeness']:.2%}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Transform stage error: {e}")
            return {
                'success': False,
                'error': str(e),
                'records_count': 0,
                'data': [],
                'quality_metrics': {}
            }
    
    def _load(self, data: list) -> Dict[str, Any]:
        """Execute Load stage.
        
        Args:
            data: Cleaned and validated data
            
        Returns:
            Dictionary with load results
        """
        PipelineLogger.log_etl_stage("LOAD", "Starting data loading")
        
        try:
            load_metrics = self.supabase_loader.load_batch(data)
            
            # Consider load successful if at least some records were loaded
            # or if Supabase is not configured (skipped)
            success = (
                load_metrics.get('skipped', False) or
                load_metrics.get('success_count', 0) > 0 or
                not self.supabase_loader.is_configured()
            )
            
            result = {
                'success': success,
                'metrics': load_metrics
            }
            
            if not success:
                result['error'] = '; '.join(load_metrics.get('errors', []))
            
            PipelineLogger.log_etl_stage(
                "LOAD",
                "Data loading completed",
                success_count=load_metrics.get('success_count', 0),
                error_count=load_metrics.get('error_count', 0)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Load stage error: {e}")
            return {
                'success': False,
                'error': str(e),
                'metrics': {}
            }

