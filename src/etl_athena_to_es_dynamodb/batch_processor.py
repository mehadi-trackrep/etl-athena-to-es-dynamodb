# batch_processor.py
import logging
from typing import Iterator, List
from etl_athena_to_es_dynamodb.interfaces import BatchProcessor
from etl_athena_to_es_dynamodb.models import DataRecord
from etl_athena_to_es_dynamodb.exceptions import BatchProcessingError

logger = logging.getLogger(__name__)

class SimpleBatchProcessor(BatchProcessor):
    """Simple batch processor implementation (SRP)"""
    
    def process_batches(self, data_iterator: Iterator[DataRecord], 
                       batch_size: int) -> Iterator[List[DataRecord]]:
        """Process data records in batches"""
        try:
            batch = []
            record_count = 0
            
            for record in data_iterator:
                batch.append(record)
                record_count += 1
                
                if len(batch) >= batch_size:
                    logger.debug(f"Yielding batch of {len(batch)} records")
                    yield batch
                    batch = []
            
            # Yield remaining records
            if batch:
                logger.debug(f"Yielding final batch of {len(batch)} records")
                yield batch
            
            logger.info(f"Batch processing completed. Total records: {record_count}")
                
        except Exception as e:
            logger.error(f"Error during batch processing: {str(e)}")
            raise BatchProcessingError(f"Batch processing failed: {str(e)}")