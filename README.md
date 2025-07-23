An etl pipeline - fetch data from AWS Athena and store them into ES/OpenSearch & DynamoDB.
It uses `uv` package manager.

### Installation Guidelines
```
    1. uv init etl-athena-to-es-dynamodb --python 3.13

    2. uv venv

    3. source .venv/bin/activate.fish

    4. uv pip install -r requirements.txt

    5. add this:- 
    [project.scripts]
    start_etl = "etl_athena_to_es_dynamodb.main:main"

    6. uv pip install -e .
    This makes etl_athena_to_es_dynamodb importable and registers the start_etl script.

    7. uv run start_etl
```

### Quick Start
```
    uv run start_etl
```