from typing import List, Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated


class AwsPlugin(Plugin):
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_region: str


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
    paths: Optional[List[str]] = []


class QuicksightPlugin(AwsPlugin):
    type: Literal["quicksight"]


class SagemakerPlugin(AwsPlugin):
    type: Literal["sagemaker"]
    experiments: Optional[List[str]]


class SagemakerFeaturestorePlugin(AwsPlugin):
    type: Literal["sagemaker_featurestore"]


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
    ],
    pydantic.Field(discriminator="type"),
]
