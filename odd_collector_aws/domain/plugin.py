from typing import List, Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated

from odd_collector_aws.domain.dataset_config import DatasetConfig


class AwsPlugin(Plugin):
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_region: str
    aws_session_token: Optional[str]
    aws_account_id: Optional[str]
    endpoint_url: Optional[str]


class GluePlugin(AwsPlugin):
    type: Literal["glue"]


class DynamoDbPlugin(AwsPlugin):
    type: Literal["dynamodb"]
    exclude_tables: Optional[List[str]] = []


class AthenaPlugin(AwsPlugin):
    type: Literal["athena"]


class SQSPlugin(AwsPlugin):
    type: Literal["sqs"]


class S3Plugin(AwsPlugin):
    type: Literal["s3"]
    endpoint_url: Optional[str] = None
    datasets: Optional[List[DatasetConfig]] = []


class QuicksightPlugin(AwsPlugin):
    type: Literal["quicksight"]


class SagemakerPlugin(AwsPlugin):
    type: Literal["sagemaker"]
    experiments: Optional[List[str]]


class SagemakerFeaturestorePlugin(AwsPlugin):
    type: Literal["sagemaker_featurestore"]


class KinesisPlugin(AwsPlugin):
    type: Literal["kinesis"]
    aws_account_id: str


AvailablePlugin = Annotated[
    Union[
        GluePlugin,
        DynamoDbPlugin,
        AthenaPlugin,
        S3Plugin,
        QuicksightPlugin,
        SagemakerFeaturestorePlugin,
        SagemakerPlugin,
        SQSPlugin,
        KinesisPlugin,
    ],
    pydantic.Field(discriminator="type"),
]
