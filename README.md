An etl pipeline - fetch data from AWS Athena and store them into ES/OpenSearch & DynamoDB.
It uses `uv` package manager.

### Installation Guidelines
```
    1. uv init etl-athena-to-es-dynamodb --python 3.13

    2. uv venv

    3. uv pip install -r requirements.txt

    4. add this:- 
    [project.scripts]
    start_etl = "etl_athena_to_es_dynamodb.main:main"

    5. uv pip install -e .
    This makes etl_athena_to_es_dynamodb importable and registers the start_etl script.

    6. uv run start_etl
```

### Quick Start
```
    uv run start_etl
```