import os
import dotenv
dotenv.load_dotenv()
import time
import boto3

class ECS:
    def __init__(self):
        self.ecs_client = boto3.client(
            'ecs',
            region_name=os.getenv('AWS_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )


def run_ecs_task(task_definition, container_name, environment):
    ecs_client = ECS().ecs_client
    response = ecs_client.run_task(
        cluster='data-team-ecs',
        taskDefinition=task_definition,
        launchType='FARGATE',
        overrides={
            'containerOverrides': [
                {
                    'name': container_name,
                    'environment': environment
                }
            ]
        },
        networkConfiguration={
            'awsvpcConfiguration': {
                'securityGroups': [
                    'sg-f3ff268f',
                ],
                'subnets': [
                    'subnet-26c5c842',
                    'subnet-42ddde34'
                ],
                'assignPublicIp': 'DISABLED'
            }
        }
    )
    print("response: ", response)


def run_ecs_task_backpop():
    # common-for-all:11
    run_ecs_task(
        task_definition='common-for-all:11',
        container_name='common-for-all',
        environment=[
            {
                'name': 'AWS_ACCESS_KEY_ID',
                'value': os.getenv('AWS_ACCESS_KEY_ID')
            },
            {
                'name': 'AWS_SECRET_ACCESS_KEY',
                'value': os.getenv('AWS_SECRET_ACCESS_KEY')
            },
            {
                'name': 'AWS_DEFAULT_REGION',
                'value': os.getenv('AWS_REGION')
            },
            {
                'name': 'AWS_ACCESS_KEY_ID_',
                'value': os.getenv('AWS_ACCESS_KEY_ID')
            },
            {
                'name': 'AWS_SECRET_ACCESS_KEY_',
                'value': os.getenv('AWS_SECRET_ACCESS_KEY')
            },
            {
                'name': 'AWS_DEFAULT_REGION_',
                'value': os.getenv('AWS_REGION')
            },
            {
                "name": "PYTHONPATH",
                "value": "/app/src"
            },
            {
                "name": "UV_SYSTEM_PYTHON",
                "value": "1"
            },
            {
                "name": "AWS_ACCOUNT_ID",
                "value": os.getenv('AWS_ACCOUNT_ID')
            },
            {
                "name": "ATHENA_DATABASE",
                "value": "TEST"
            },
            {
                "name": "ATHENA_TABLE",
                "value": "TEST"
            },
            {
                "name": "ATHENA_S3_OUTPUT_LOCATION",
                "value": "s3://goava-dev/athena-queries/output"
            },
            {
                "name": "ATHENA_WORK_GROUP",
                "value": "primary"
            },
            {
                "name": "OPENSEARCH_DOCUMENT_TYPE",
                "value": "child"
            },
            {
                "name": "OPENSEARCH_CHILD_RELATION_TYPE",
                "value": "vehicle"
            },
            {
                "name": "DYNAMODB_TABLE_NAME",
                "value": "company_v0.03"
            },
            {
                "name": "DYNAMODB_OVERWRITE_BY_PKEYS",
                "value": "orgno"
            },
            {
                "name": "OPENSEARCH_INDEX",
                "value": "data"
            },
            {
                "name": "OPENSEARCH_ENDPOINT",
                "value": os.getenv('OPENSEARCH_ENDPOINT')
            }
        ]
    )

    
if __name__ == '__main__':
    run_ecs_task_backpop()

    
    