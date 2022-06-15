import logging
from typing import List

import boto3
from oddrn_generator.generators import S3Generator
from urllib.parse import urlparse
from odd_collector_aws.domain.plugin import S3Plugin
from odd_collector_aws.adapters.s3.compatible_generator import S3CompatibleGenerator


class Client:
    """
    Client hides boto3 implementation details.
    """

    def __init__(self, config: S3Plugin):
        self._config = config
        self._client = boto3.client(
            "s3",
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.endpoint_url,
        )
        self._oddrn_generator = self.get_oddrn_generator()

    def get_oddrn_generator(self):
        try:
            account_id = (
                boto3.client(
                    "sts",
                    aws_access_key_id=self._config.aws_access_key_id,
                    aws_secret_access_key=self._config.aws_secret_access_key,
                    endpoint_url=self._config.endpoint_url,
                )
                .get_caller_identity()
                .get("Account")
            )
            return S3Generator(
                cloud_settings={
                    "region": self._config.aws_region,
                    "account": account_id,
                }
            )
        except Exception:
            logging.error("Could not create S3Generator, using S3 compatible")
            endpoint_url = urlparse(self._config.endpoint_url).hostname
            return S3CompatibleGenerator(host_settings=endpoint_url)

    @property
    def client(self):
        return self._client

    @property
    def oddrn_generator(self):
        return self._oddrn_generator

    def list_objects(self, bucket: str, prefix: str) -> List[dict]:
        """
        List file objects in a bucket. Minio doesn't return folders as objects.
        """
        try:

            objects = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)[
                "Contents"
            ]
        except KeyError as e:
            raise KeyError(f"Wrong S3 path {bucket}/{prefix}") from e
        else:
            return [e for e in objects if e.get("Size")]
