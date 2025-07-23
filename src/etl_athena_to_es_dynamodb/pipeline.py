# pipeline.py
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from etl_athena_to_es_dynamodb.interfaces import DataSource, DataSink, BatchProcessor
from etl_athena_to_es_dynamodb.models import BatchConfig, BatchResult
from etl_athena_to_es_dynamodb.exceptions import DataPipelineError

logger = logging.getLogger(__name__)

class DataPipeline:
    """Main data pipeline orchestrator (SRP, DIP)"""
    
    def __init__(self, 
                 data_source: DataSource,
                 data_sinks: List[DataSink],
                 batch_processor: BatchProcessor,
                 batch_config: BatchConfig):
        self.data_source = data_source
        self.data_sinks = data_sinks
        self.batch_processor = batch_processor
        self.batch_config = batch_config
        logger.info(f"DataPipeline initialized with {len(data_sinks)} sinks")
    
    def execute(self, query: str) -> Dict[str, Any]:
        """Execute the data pipeline"""
        try:
            logger.info("Starting data pipeline execution")
            logger.info(f"self.data_source: {self.data_source}")
            
            # Fetch data from source
            data_iterator = self.data_source.fetch_data(query)
            
            # Process data in batches
            batches = self.batch_processor.process_batches(
                data_iterator, 
                self.batch_config.batch_size
            )
            
            # Process batches concurrently across all sinks
            pipeline_results = self._process_batches_concurrently(batches)
            
            logger.info("Data pipeline execution completed successfully")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise DataPipelineError(f"Pipeline execution failed: {str(e)}")
        finally:
            self._cleanup_resources()
    
    def _process_batches_concurrently(self, batches) -> Dict[str, Any]:
        """Process batches concurrently across all sinks"""
        total_batches = 0
        sink_results = {sink.__class__.__name__: [] for sink in self.data_sinks}
        
        with ThreadPoolExecutor(max_workers=self.batch_config.max_workers) as executor:
            for batch in batches:
                total_batches += 1
                logger.info(f"Processing batch {total_batches} with {len(batch)} records")
                
                # Submit batch to all sinks concurrently
                future_to_sink = {}
                for sink in self.data_sinks:
                    logger.info(f"Submitting batch to sink: {sink.__class__.__name__}")
                    logger.info(f"Batch data size: {len(batch)}")
                    future = executor.submit(sink.upsert_batch, batch)
                    future_to_sink[future] = sink.__class__.__name__
                
                # Collect results from all sinks
                for future in as_completed(future_to_sink):
                    sink_name = future_to_sink[future]
                    try:
                        result = future.result()
                        sink_results[sink_name].append(result)
                        logger.info(f"Batch completed for {sink_name}: {result.success_rate:.1f}% success rate")
                    except Exception as e:
                        logger.error(f"Error in sink {sink_name}: {str(e)}")
                        # Create failed result
                        failed_result = BatchResult(
                            total_records=len(batch),
                            successful_records=0,
                            failed_records=len(batch),
                            errors=[str(e)]
                        )
                        sink_results[sink_name].append(failed_result)
        
        # Aggregate results
        aggregated_results = self._aggregate_results(sink_results, total_batches)
        return aggregated_results
    
    def _aggregate_results(self, sink_results: Dict[str, List[BatchResult]], 
                          total_batches: int) -> Dict[str, Any]:
        """Aggregate results from all sinks"""
        aggregated = {
            'total_batches': total_batches,
            'sinks': {}
        }
        
        for sink_name, results in sink_results.items():
            total_records = sum(r.total_records for r in results)
            successful_records = sum(r.successful_records for r in results)
            failed_records = sum(r.failed_records for r in results)
            all_errors = []
            for r in results:
                all_errors.extend(r.errors)
            
            success_rate = (successful_records / total_records * 100) if total_records > 0 else 0
            
            aggregated['sinks'][sink_name] = {
                'total_records': total_records,
                'successful_records': successful_records,
                'failed_records': failed_records,
                'success_rate': round(success_rate, 2),
                'error_count': len(all_errors)
            }
        
        return aggregated
    
    def _cleanup_resources(self) -> None:
        """Cleanup all resources"""
        try:
            logger.info("Cleaning up resources")
            self.data_source.close()
            for sink in self.data_sinks:
                sink.close()
            logger.info("Resource cleanup completed")
        except Exception as e:
            logger.warning(f"Error during resource cleanup: {str(e)}")
