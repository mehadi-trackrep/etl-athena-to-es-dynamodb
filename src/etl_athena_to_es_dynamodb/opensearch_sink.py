# opensearch_sink.py
import uuid
import boto3
import logging
import traceback
from typing import List
from pydantic import ValidationError
from opensearchpy.helpers import bulk
from requests_aws4auth import AWS4Auth
import etl_athena_to_es_dynamodb.utils as utils
from opensearchpy import OpenSearch, RequestsHttpConnection
from etl_athena_to_es_dynamodb.interfaces import DataSink
from etl_athena_to_es_dynamodb.exceptions import DataSinkError, ConfigurationError
from etl_athena_to_es_dynamodb.models import DataRecord, BatchResult, OpenSearchConfig, DocumentConfig

logger = logging.getLogger(__name__)

class OpenSearchDataSink(DataSink):
    """OpenSearch data sink implementation (SRP)"""
    
    def __init__(self, config: OpenSearchConfig, document_config: DocumentConfig):
        try:
            self.config = config
            self.document_config = document_config
            self._client = None
            logger.info("OpenSearchDataSink initialized successfully")
        except ValidationError as e:
            raise ConfigurationError(f"Invalid OpenSearch configuration: {str(e)}")
    
    def get_es_auth(self, boto_session):
        credentials = boto_session.get_credentials()
        auth = AWS4Auth(
            region=self.config.region,
            service='es',
            refreshable_credentials=credentials
        )

        return auth

    @property
    def client(self) -> OpenSearch:
        """Lazy initialization of OpenSearch client"""
        if not self._client:
            self._client = OpenSearch(
                hosts=[
                    {
                        'host': self.config.endpoint,
                        'port': self.config.port
                    }
                ],
                http_auth=self.get_es_auth(boto3.Session()),
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30
            )
            logger.debug("OpenSearch client initialized")
        return self._client
    
    def upsert_batch(self, records: List[DataRecord]) -> BatchResult:
        """Insert batch of records into OpenSearch"""
        if not records:
            return BatchResult(total_records=0, successful_records=0, failed_records=0)

        document_type = self.document_config.document_type # parent or child
        child_relation_type = self.document_config.child_relation_type
        if not child_relation_type:
            raise DataSinkError("Child relation type must be specified for child documents")
        
        try:
            logger.info(f"Inserting batch of {len(records)} records into OpenSearch")
            
            logger.info(f"OpenSearch index: {self.config.index_name}")
            actions = []
            for record in records:
                item = record.to_dict()
                logger.info(f"Record to insert: {item}")
                
                if document_type.lower().strip() == "parent":
                    doc_ = {
                        "indexed_at": utils.get_utc_time()
                    }
                    action = {
                        "_op_type": "update",
                        '_index': self.config.index_name,
                        "_id": item['orgno'],
                        "_routing": item['orgno'],
                        "doc": doc_ | {k: v for k, v in item.items() if k != 'orgno' or v}
                    }
                    actions.append(action)
                
                if document_type.lower().strip() == "child":
                    doc_rel = {
                        "relation_type": {
                            "name": child_relation_type,
                            "parent": item['orgno']
                        },
                        "indexed_at": utils.get_utc_time()
                    }
                    child_data_list = item['child_data']
                    for child_doc in child_data_list:
                        action = {
                            "_op_type": "update",
                            '_index': self.config.index_name,
                            "_id": uuid.uuid4(), # guid
                            "_routing": item['orgno'],
                            "doc": doc_rel | {k: v for k, v in child_doc.items() if v}, # Merge dictionaries
                            "doc_as_upsert": True  # Create if doesn't exist
                        }
                        logger.info(f"\n---> Child document to insert: {action}\n")
                        actions.append(action)
            
            # Perform bulk insert
            success_count, failed_items = bulk(
                self.client, 
                actions, 
                chunk_size=len(actions),
                request_timeout=120,
                raise_on_error=False,  # Don't fail entire batch on single doc errors
                raise_on_exception=True
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
            logger.error(f"Error inserting batch into OpenSearch. Traceback: {traceback.format_exc()}")
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