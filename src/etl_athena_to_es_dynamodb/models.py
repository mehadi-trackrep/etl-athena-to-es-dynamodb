# models.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any, List
from datetime import datetime

class AWSConfig(BaseModel):
    """AWS configuration model"""
    model_config = ConfigDict(frozen=True)
    
    region: str = Field(..., description="AWS region")
    access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(None, description="AWS secret access key")

class AthenaConfig(BaseModel):
    """Athena configuration model"""
    model_config = ConfigDict(frozen=True)
    
    database: str = Field(..., description="Athena database name")
    table: str = Field(..., description="Athena table name")
    s3_output_location: str = Field(..., description="S3 location for query results")
    work_group: str = Field(default="primary", description="Athena work group")

class OpenSearchConfig(BaseModel):
    """OpenSearch configuration model"""
    model_config = ConfigDict(frozen=True)
    
    endpoint: str = Field(..., description="OpenSearch endpoint URL")
    index_name: str = Field(..., description="OpenSearch index name")
    username: Optional[str] = Field(None, description="OpenSearch username")
    password: Optional[str] = Field(None, description="OpenSearch password")

class DynamoDBConfig(BaseModel):
    """DynamoDB configuration model"""
    model_config = ConfigDict(frozen=True)
    
    table_name: str = Field(..., description="DynamoDB table name")

class BatchConfig(BaseModel):
    """Batch processing configuration model"""
    model_config = ConfigDict(frozen=True)
    
    batch_size: int = Field(default=25, ge=1, le=100, description="Batch size for processing")
    max_workers: int = Field(default=4, ge=1, le=10, description="Maximum worker threads")

class DataRecord(BaseModel):
    """Generic data record model"""
    model_config = ConfigDict(extra='allow')
    
    data: Dict[str, Any] = Field(..., description="Record data")
    
    @classmethod
    def from_dict(cls, record_dict: Dict[str, Any]) -> 'DataRecord':
        """Create DataRecord from dictionary"""
        return cls(data=record_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.data

class BatchResult(BaseModel):
    """Batch processing result model"""
    total_records: int = Field(..., description="Total number of records processed")
    successful_records: int = Field(..., description="Number of successfully processed records")
    failed_records: int = Field(..., description="Number of failed records")
    errors: List[str] = Field(default_factory=list, description="List of error messages")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.successful_records / self.total_records) * 100