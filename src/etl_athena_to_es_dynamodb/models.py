# models.py
import ast
import json
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
    
    endpoint: str = Field(..., description="OpenSearch endpoint URL") # host
    index_name: str = Field(..., description="OpenSearch index name")
    port: Optional[int] = Field(1234, ge=1, le=65535, description="OpenSearch port number")
    # username: Optional[str] = Field(None, description="OpenSearch username")
    # password: Optional[str] = Field(None, description="OpenSearch password")

class DynamoDBConfig(BaseModel):
    """DynamoDB configuration model"""
    model_config = ConfigDict(frozen=True)
    
    table_name: str = Field(..., description="DynamoDB table name")
    overwrite_by_pkeys: List[str] = Field(default_factory=list, description="List of primary keys to overwrite existing records")

class BatchConfig(BaseModel):
    """Batch processing configuration model"""
    model_config = ConfigDict(frozen=True)
    
    batch_size: int = Field(default=100, ge=1, le=1000, description="Batch size for processing")
    max_workers: int = Field(default=4, ge=1, le=10, description="Maximum worker threads")

class DataRecord(BaseModel):
    """Generic data record model"""
    model_config = ConfigDict(extra='allow')
    
    data: Dict[str, Any] = Field(..., description="Record data")
    
    @classmethod
    def from_dict(cls, record_dict: Dict[str, Any]) -> 'DataRecord':
        """Create DataRecord from dictionary"""
        return cls(data=record_dict)

    def convert_object_to_dict(self, obj: dict) -> Dict[str, Any]:
        """
        Convert an object to Python dictionary with proper type casting.
        Fields with string values that look like arrays '[{...}]' are converted to actual arrays.
        """
        result = {}
        
        for key, value in obj.items():
            if isinstance(value, str):
                # Check if the string value looks like an array (starts with [ and ends with ])
                stripped_value = value.strip()
                if stripped_value.startswith('[') and stripped_value.endswith(']'):
                    try:
                        # Try to parse as JSON first (more reliable)
                        result[key] = json.loads(stripped_value)
                    except json.JSONDecodeError:
                        try:
                            # Fallback to ast.literal_eval for Python-like syntax
                            result[key] = ast.literal_eval(stripped_value)
                        except (ValueError, SyntaxError):
                            # If parsing fails, keep as string
                            result[key] = value
                else:
                    result[key] = value
            else:
                # Keep non-string values as they are
                result[key] = value
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.convert_object_to_dict(self.data)

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