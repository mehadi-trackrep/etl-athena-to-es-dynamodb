# interfaces.py
from abc import ABC, abstractmethod
from typing import Iterator, List
from etl_athena_to_es_dynamodb.models import DataRecord, BatchResult

class DataSource(ABC):
    """Abstract interface for data sources (ISP)"""
    
    @abstractmethod
    def fetch_data(self, query: str) -> Iterator[DataRecord]:
        """Fetch data from the source"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection to the source"""
        pass

class DataSink(ABC):
    """Abstract interface for data sinks (ISP)"""
    
    @abstractmethod
    def upsert_batch(self, records: List[DataRecord]) -> BatchResult:
        """Upsert a batch of records"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection to the sink"""
        pass

class BatchProcessor(ABC):
    """Abstract interface for batch processing (ISP)"""
    
    @abstractmethod
    def process_batches(self, data_iterator: Iterator[DataRecord], 
                       batch_size: int) -> Iterator[List[DataRecord]]:
        """Process data in batches"""
        pass