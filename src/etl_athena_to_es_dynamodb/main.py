# main.py
import os
import logging
from dotenv import load_dotenv
from etl_athena_to_es_dynamodb.models import (AWSConfig, AthenaConfig, OpenSearchConfig, 
                   DynamoDBConfig, BatchConfig)
from etl_athena_to_es_dynamodb.pipeline_factory import PipelineFactory
from etl_athena_to_es_dynamodb.exceptions import DataPipelineError, ConfigurationError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_configuration():
    """Load configuration from environment variables"""
    try:
        aws_config = AWSConfig(
            region=os.getenv('AWS_REGION', 'us-east-1'),
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        athena_config = AthenaConfig(
            database=os.getenv('ATHENA_DATABASE'),
            table=os.getenv('ATHENA_TABLE'),
            s3_output_location=os.getenv('ATHENA_S3_OUTPUT_LOCATION'),
            work_group=os.getenv('ATHENA_WORK_GROUP', 'primary')
        )
        
        opensearch_config = None
        if os.getenv('OPENSEARCH_ENDPOINT'):
            opensearch_config = OpenSearchConfig(
                endpoint=os.getenv('OPENSEARCH_ENDPOINT'),
                index_name=os.getenv('OPENSEARCH_INDEX', 'data_index'),
                username=os.getenv('OPENSEARCH_USERNAME'),
                password=os.getenv('OPENSEARCH_PASSWORD')
            )
        
        dynamodb_config = None
        if os.getenv('DYNAMODB_TABLE_NAME'):
            dynamodb_config = DynamoDBConfig(
                table_name=os.getenv('DYNAMODB_TABLE_NAME')
            )
        
        batch_config = BatchConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '25')),
            max_workers=int(os.getenv('MAX_WORKERS', '4'))
        )
        
        return aws_config, athena_config, opensearch_config, dynamodb_config, batch_config
        
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {str(e)}")

def main():
    """Main function to execute the data pipeline"""
    try:
        logger.info("Starting AWS Data Pipeline")
        
        # Load configuration
        aws_config, athena_config, opensearch_config, dynamodb_config, batch_config = load_configuration()
        
        # Create pipeline
        pipeline = PipelineFactory.create_pipeline(
            aws_config=aws_config,
            athena_config=athena_config,
            opensearch_config=opensearch_config,
            dynamodb_config=dynamodb_config,
            batch_config=batch_config
        )
        
        # Define query
        query = f"""
        SELECT * 
        FROM {athena_config.database}.{athena_config.table} 
        LIMIT {os.getenv('QUERY_LIMIT', '1000')}
        """
        
        logger.info(f"Executing query: {query}")
        
        # Execute pipeline
        results = pipeline.execute(query)
        
        # Log results
        logger.info("=== Pipeline Execution Results ===")
        logger.info(f"Total batches processed: {results['total_batches']}")
        
        for sink_name, sink_results in results['sinks'].items():
            logger.info(f"\n{sink_name} Results:")
            logger.info(f"  Total records: {sink_results['total_records']}")
            logger.info(f"  Successful: {sink_results['successful_records']}")
            logger.info(f"  Failed: {sink_results['failed_records']}")
            logger.info(f"  Success rate: {sink_results['success_rate']}%")
            logger.info(f"  Errors: {sink_results['error_count']}")
        
        logger.info("Pipeline execution completed successfully")
        
    except (ConfigurationError, DataPipelineError) as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise DataPipelineError(f"Unexpected pipeline failure: {str(e)}")

if __name__ == "__main__":
    main()
