# opensearch_sink.py
import logging
from typing import List
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
from pydantic import ValidationError
from etl_athena_to_es_dynamodb.interfaces import DataSink
from etl_athena_to_es_dynamodb.models import DataRecord, BatchResult, OpenSearchConfig
from etl_athena_to_es_dynamodb.exceptions import DataSinkError, ConfigurationError

logger = logging.getLogger(__name__)

class OpenSearchDataSink(DataSink):
    """OpenSearch data sink implementation (SRP)"""
    
    def __init__(self, config: OpenSearchConfig):
        try:
            self.config = config
            self._client = None
            logger.info("OpenSearchDataSink initialized successfully")
        except ValidationError as e:
            raise ConfigurationError(f"Invalid OpenSearch configuration: {str(e)}")
    
    @property
    def client(self) -> OpenSearch:
        """Lazy initialization of OpenSearch client"""
        if self._client is None:
            auth = None
            if self.config.username and self.config.password:
                auth = (self.config.username, self.config.password)
            
            self._client = OpenSearch(
                hosts=[self.config.endpoint],
                http_auth=auth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30
            )
            logger.debug("OpenSearch client initialized")
        return self._client
    
    def insert_batch(self, records: List[DataRecord]) -> BatchResult:
        """Insert batch of records into OpenSearch"""
        if not records:
            return BatchResult(total_records=0, successful_records=0, failed_records=0)
        
        try:
            logger.info(f"Inserting batch of {len(records)} records into OpenSearch")
            
            # Prepare documents for bulk insert
            actions = []
            for record in records:
                action = {
                    '_index': self.config.index_name,
                    '_source': record.to_dict()
                }
                actions.append(action)
            
            # Perform bulk insert
            success_count, failed_items = bulk(
                self.client, 
                actions, 
                chunk_size=len(actions),
                request_timeout=30
            )
            
            failed_count = len(failed_items) if failed_items else 0
            errors = [str(item) for item in failed_items] if failed_items else []
            
            result = BatchResult(
                total_records=len(records),
                successful_records=success_count,
                failed_records=failed_count,
                errors=errors
            )
            
            logger.info(f"OpenSearch batch insert completed: {success_count} success, {failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"Error inserting batch into OpenSearch: {str(e)}")
            return BatchResult(
                total_records=len(records),
                successful_records=0,
                failed_records=len(records),
                errors=[str(e)]
            )
    
    def close(self) -> None:
        """Close OpenSearch connection"""
        if self._client:
            try:
                self._client.transport.close()
            except Exception as e:
                logger.warning(f"Error closing OpenSearch connection: {str(e)}")
        self._client = None
        logger.info("OpenSearch connection closed")