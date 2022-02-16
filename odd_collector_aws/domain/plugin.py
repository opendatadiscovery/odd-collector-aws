from typing import List, Literal, Optional, Union
import pydantic
from typing_extensions import Annotated

from odd_collector_sdk.domain.plugin import Plugin


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


class S3Plugin(AwsPlugin):
    type: Literal["s3"]
    buckets: Optional[List[str]] = []


class QuicksightPlugin(AwsPlugin):
    type: Literal["quicksight"]


class SagemakerPlugin(AwsPlugin):
    type: Literal["sagemaker_featurestore"]


AvailablePlugin = Annotated[
    Union[
        GluePlugin,
        DynamoDbPlugin,
        AthenaPlugin,
        S3Plugin,
        QuicksightPlugin,
        SagemakerPlugin,
    ],
    pydantic.Field(discriminator="type"),
]