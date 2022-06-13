import logging
from typing import List

import boto3

from odd_collector_aws.domain.plugin import S3Plugin


class Client:
    """
    Client hides boto3 implementation details.
    """

    def __init__(self, config: S3Plugin):
        self.client = boto3.client(
            "s3",
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.endpoint_url,
        )

        try:
            self.account_id = (
                boto3.client(
                    "sts",
                    aws_access_key_id=config.aws_access_key_id,
                    aws_secret_access_key=config.aws_secret_access_key,
                    endpoint_url=config.endpoint_url,
                )
                .get_caller_identity()
                .get("Account")
            )
        except Exception:
            logging.error('could not get account id, use default "unknown"')
            self.account_id = "unknown"

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
