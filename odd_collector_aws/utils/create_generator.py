import logging
from typing import Type
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from oddrn_generator import Generator
from oddrn_generator.generators import S3Generator

from odd_collector_aws.aws.aws_client import AwsClient
from odd_collector_aws.utils.s3_compatible_generator import S3CompatibleGenerator
from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.errors import AccountIdError


def create_generator(
    generator_cls: Type[Generator], aws_plugin: AwsPlugin
) -> Generator:
    aws_client = AwsClient(aws_plugin)

    region = aws_plugin.aws_region
    account_id = aws_plugin.aws_account_id

    if account_id is None:
        try:
            account_id = aws_client.get_account_id()
        except ClientError as e:
            logging.debug(e)
            raise AccountIdError from e

    if generator_cls != S3Generator or not aws_plugin.endpoint_url:
        return generator_cls(cloud_settings={"region": region, "account": account_id})
    endpoint_url = urlparse(aws_plugin.endpoint_url).hostname
    return S3CompatibleGenerator(host_settings=endpoint_url)
