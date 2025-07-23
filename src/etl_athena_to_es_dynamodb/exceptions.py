# exceptions.py
class DataPipelineError(Exception):
    """Base exception for data pipeline errors"""
    pass

class DataSourceError(DataPipelineError):
    """Exception raised when data source operations fail"""
    pass

class DataSinkError(DataPipelineError):
    """Exception raised when data sink operations fail"""
    pass

class BatchProcessingError(DataPipelineError):
    """Exception raised during batch processing"""
    pass

class ConfigurationError(DataPipelineError):
    """Exception raised for configuration errors"""
    pass