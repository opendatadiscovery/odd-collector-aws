from typing import List, Literal, Optional

from odd_collector_sdk.domain.plugin import Plugin

from odd_collector_aws.domain.dataset_config import DatasetConfig
from odd_collector_sdk.types import PluginFactory


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


PLUGIN_FACTORY: PluginFactory = {
    "glue": GluePlugin,
    "dynamodb": DynamoDbPlugin,
    "athena": AthenaPlugin,
    "sqs": SQSPlugin,
    "s3": S3Plugin,
    "quicksight": QuicksightPlugin,
    "sagemaker": SagemakerPlugin,
    "sagemaker_featurestore": SagemakerFeaturestorePlugin,
    "kinesis": KinesisPlugin,
}
