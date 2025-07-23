# dynamodb_sink.py
import boto3
import logging
import traceback
from typing import List
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import ValidationError
from etl_athena_to_es_dynamodb.interfaces import DataSink
from etl_athena_to_es_dynamodb.models import DataRecord, BatchResult, AWSConfig, DynamoDBConfig
from etl_athena_to_es_dynamodb.exceptions import DataSinkError, ConfigurationError

logger = logging.getLogger(__name__)

class DynamoDBDataSink(DataSink):
    """DynamoDB data sink implementation (SRP)"""
    
    def __init__(self, aws_config: AWSConfig, dynamodb_config: DynamoDBConfig):
        try:
            self.aws_config = aws_config
            self.dynamodb_config = dynamodb_config
            self._resource = None
            self._table = None
            logger.info("DynamoDBDataSink initialized successfully")
        except ValidationError as e:
            raise ConfigurationError(f"Invalid DynamoDB configuration: {str(e)}")
    
    @property
    def table(self):
        """Lazy initialization of DynamoDB table resource"""
        if self._table is None:
            session = boto3.Session(
                aws_access_key_id=self.aws_config.access_key_id,
                aws_secret_access_key=self.aws_config.secret_access_key,
                region_name=self.aws_config.region
            )
            self._resource = session.resource('dynamodb')
            self._table = self._resource.Table(self.dynamodb_config.table_name)
            logger.debug("DynamoDB table resource initialized")
        return self._table
    
    @staticmethod
    def __generate_key_from_orgno(orgno):
        key = {
            'orgno': int(orgno)
        }

        return key

    @staticmethod
    def __get_attribute_updates(item) -> dict:
        attribute_updates = dict()

        for key in item:
            if key == 'orgno':
                continue
            attribute_updates[key] = {
                "Value": item[key]
            }
        
        return attribute_updates
    
    def upsert_batch(self, records: List[DataRecord]) -> BatchResult:
        """Upsert batch of records into DynamoDB"""
        if not records:
            return BatchResult(total_records=0, successful_records=0, failed_records=0)
        
        try:
            logger.info(f"Upserting batch of {len(records)} records into DynamoDB")
            
            successful_count = 0
            failed_count = 0
            errors = []
            
            for record in records:
                try:
                    item = record.to_dict()
                    attribute_updates = self.__get_attribute_updates(item=item)

                    _ = self.table.update_item(
                        Key=self.__generate_key_from_orgno(orgno=item.get('orgno')),
                        AttributeUpdates=attribute_updates
                    )
                    successful_count += 1
                    if successful_count % 1000 == 0:
                        logger.info(f"Successfully upserted {successful_count} records so far")
                except (BotoCoreError, ClientError) as e:
                    failed_count += 1
                    errors.append(f"Failed to upsert record: {str(e)}")
                    logger.warning(f"Failed to upsert record into DynamoDB: {str(e)}")
        
            result = BatchResult(
                total_records=len(records),
                successful_records=successful_count,
                failed_records=failed_count,
                errors=errors
            )
            
            logger.info(f"DynamoDB batch upsert completed: {successful_count} success, {failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"Error upserting batch into DynamoDB. Traceback: {traceback.format_exc()}")
            return BatchResult(
                total_records=len(records),
                successful_records=0,
                failed_records=len(records),
                errors=[str(e)]
            )

    def close(self) -> None:
        """Close DynamoDB connections"""
        self._resource = None
        self._table = None
        logger.info("DynamoDB connections closed")