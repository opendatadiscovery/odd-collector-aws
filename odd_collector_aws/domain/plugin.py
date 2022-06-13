from typing import List, Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from pydantic import validator
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


class DatasetConfig(pydantic.BaseModel):
    bucket: str
    path: str
    partitioning: Optional[str] = None
    each_file_as_dataset: bool = False

    @property
    def full_path(self) -> str:
        return f"{self.bucket}/{self.path}"

    @validator("each_file_as_dataset")
    def validate_each_file_as_dataset(cls, v: Optional[bool], values) -> Optional[int]:
        if not values.get("path").endswith("/"):
            raise ValueError("For each_file_as_dataset, path must end with '/'")

        if values.get("path") == "/" and not v:
            raise ValueError("For root bucket path, each_file_as_dataset must be True")

        return v

    @validator("partitioning")
    def validate_partitioning(cls, v: Optional[bool], values) -> Optional[int]:
        if not values.get("path").endswith("/"):
            raise ValueError("For partitioning, path must end with '/'")
        return v


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
