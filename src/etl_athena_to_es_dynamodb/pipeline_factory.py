# pipeline_factory.py
import logging
from typing import List, Optional
from etl_athena_to_es_dynamodb.models import (AWSConfig, AthenaConfig, OpenSearchConfig, 
                   DocumentConfig, DynamoDBConfig, BatchConfig)
from etl_athena_to_es_dynamodb.athena_source import AthenaDataSource
from etl_athena_to_es_dynamodb.opensearch_sink import OpenSearchDataSink
from etl_athena_to_es_dynamodb.dynamodb_sink import DynamoDBDataSink
from etl_athena_to_es_dynamodb.batch_processor import SimpleBatchProcessor
from etl_athena_to_es_dynamodb.pipeline import DataPipeline
from etl_athena_to_es_dynamodb.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

class PipelineFactory:
    """Factory for creating data pipeline instances (Factory Pattern, SRP)"""
    
    @staticmethod
    def create_pipeline(
        aws_config: AWSConfig,
        athena_config: AthenaConfig,
        document_config: DocumentConfig,
        opensearch_config: Optional[OpenSearchConfig] = None,
        dynamodb_config: Optional[DynamoDBConfig] = None,
        batch_config: Optional[BatchConfig] = None
    ) -> DataPipeline:
        """Create a configured data pipeline"""
        
        # Validate that at least one sink is configured
        if not opensearch_config and not dynamodb_config:
            raise ConfigurationError("At least one sink (OpenSearch or DynamoDB) must be configured")
        
        logger.info("Creating data pipeline components")
        
        # Create data source
        data_source = AthenaDataSource(aws_config, athena_config)
        
        # Create data sinks
        data_sinks = []
        if opensearch_config:
            data_sinks.append(OpenSearchDataSink(opensearch_config, document_config))
            logger.info("OpenSearch sink added to pipeline")
        if dynamodb_config:
            data_sinks.append(DynamoDBDataSink(aws_config, dynamodb_config, document_config))
            logger.info("DynamoDB sink added to pipeline")
        
        # Create batch processor
        batch_processor = SimpleBatchProcessor()
        
        # Use default batch config if not provided
        if batch_config is None:
            batch_config = BatchConfig()
        
        logger.info(f"Pipeline created with {len(data_sinks)} sinks, batch size: {batch_config.batch_size}")
        
        return DataPipeline(
            data_source=data_source,
            data_sinks=data_sinks,
            batch_processor=batch_processor,
            batch_config=batch_config
        )