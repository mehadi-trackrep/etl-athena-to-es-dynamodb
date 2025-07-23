# athena_source.py
import boto3
import time
import logging
from typing import Iterator, Dict, Any
from pydantic import ValidationError
from etl_athena_to_es_dynamodb.interfaces import DataSource
from etl_athena_to_es_dynamodb.models import DataRecord, AWSConfig, AthenaConfig
from etl_athena_to_es_dynamodb.exceptions import DataSourceError, ConfigurationError

logger = logging.getLogger(__name__)

class AthenaDataSource(DataSource):
    """Athena data source implementation (SRP)"""
    
    def __init__(self, aws_config: AWSConfig, athena_config: AthenaConfig):
        try:
            self.aws_config = aws_config
            self.athena_config = athena_config
            self._athena_client = None
            self._s3_client = None
            logger.info("AthenaDataSource initialized successfully")
        except ValidationError as e:
            raise ConfigurationError(f"Invalid configuration: {str(e)}")
    
    @property
    def athena_client(self):
        """Lazy initialization of Athena client"""
        if self._athena_client is None:
            session = boto3.Session(
                aws_access_key_id=self.aws_config.access_key_id,
                aws_secret_access_key=self.aws_config.secret_access_key,
                region_name=self.aws_config.region
            )
            self._athena_client = session.client('athena')
            logger.debug("Athena client initialized")
        return self._athena_client
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client"""
        if self._s3_client is None:
            session = boto3.Session(
                aws_access_key_id=self.aws_config.access_key_id,
                aws_secret_access_key=self.aws_config.secret_access_key,
                region_name=self.aws_config.region
            )
            self._s3_client = session.client('s3')
            logger.debug("S3 client initialized")
        return self._s3_client
    
    def fetch_data(self, query: str) -> Iterator[DataRecord]:
        """Fetch data from Athena table"""
        try:
            logger.info(f"Starting Athena query execution")
            
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_config.database},
                ResultConfiguration={'OutputLocation': self.athena_config.s3_output_location},
                WorkGroup=self.athena_config.work_group
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Query execution started with ID: {query_execution_id}")
            
            # Wait for query completion
            self._wait_for_query_completion(query_execution_id)
            
            # Fetch and yield results
            yield from self._fetch_query_results(query_execution_id)
            
            logger.info("Data fetching completed successfully")
            
        except Exception as e:
            logger.error(f"Error fetching data from Athena: {str(e)}")
            raise DataSourceError(f"Failed to fetch data from Athena: {str(e)}")
    
    def _wait_for_query_completion(self, query_execution_id: str) -> None:
        """Wait for Athena query to complete"""
        max_wait_time = 300  # 5 minutes
        wait_interval = 2
        total_wait = 0
        
        logger.info("Waiting for query completion...")
        
        while total_wait < max_wait_time:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                logger.info("Query completed successfully")
                return
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise DataSourceError(f"Query {status.lower()}: {reason}")
            
            time.sleep(wait_interval)
            total_wait += wait_interval
        
        raise DataSourceError(f"Query timeout after {max_wait_time} seconds")
    
    def _fetch_query_results(self, query_execution_id: str) -> Iterator[DataRecord]:
        """Fetch results from completed Athena query"""
        paginator = self.athena_client.get_paginator('get_query_results')
        
        headers = []
        first_page = True
        record_count = 0
        
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page['ResultSet']['Rows']
            
            if first_page:
                # Extract column names from the first row
                headers = [col.get('VarCharValue', '') for col in rows[0]['Data']]
                rows = rows[1:]  # Skip header row
                first_page = False
                logger.info(f"Query returned {len(headers)} columns: {headers}")
            
            for row in rows:
                try:
                    data = {}
                    for i, col in enumerate(row['Data']):
                        value = col.get('VarCharValue', '')
                        if i < len(headers):
                            data[headers[i]] = value
                    
                    yield DataRecord.from_dict(data)
                    record_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing row: {str(e)}")
                    continue
        
        logger.info(f"Fetched {record_count} records from Athena")
    
    def close(self) -> None:
        """Close Athena connections"""
        self._athena_client = None
        self._s3_client = None
        logger.info("Athena connections closed")
