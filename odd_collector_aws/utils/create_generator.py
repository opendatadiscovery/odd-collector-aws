import logging
from typing import Type, TypeVar

from botocore.exceptions import ClientError
from oddrn_generator import Generator
from oddrn_generator.generators import S3CustomGenerator, S3Generator

from odd_collector_aws.aws.aws_client import Aws
from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.errors import AccountIdError

T = TypeVar("T", bound=Generator)


def create_generator(generator_cls: Type[T], aws_plugin: AwsPlugin) -> T:
    aws_client = Aws(aws_plugin)

    if generator_cls == S3Generator:
        if aws_plugin.endpoint_url:
            return S3CustomGenerator(endpoint=aws_plugin.endpoint_url)

        return generator_cls()

    account_id = aws_plugin.aws_account_id

    if not account_id:
        try:
            account_id = aws_client.get_account_id()
        except ClientError as e:
            logging.debug(e)
            raise AccountIdError from e

    return generator_cls(
        cloud_settings={"region": aws_plugin.aws_region, "account": account_id}
    )
