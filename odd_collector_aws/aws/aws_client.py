from boto3 import Session
from botocore.client import BaseClient

from odd_collector_aws.domain.plugin import AwsPlugin

from logging import logger


class AwsClient:
    session: Session

    def __init__(self, config: AwsPlugin):
        self._config = config

        self._init_session()

    def _init_session(self):

        self.session = Session(
            aws_access_key_id=self._config.aws_access_key_id,
            aws_secret_access_key=self._config.aws_secret_access_key,
            aws_session_token=self._config.aws_session_token,
            region_name=self._config.aws_region,
        )

        if self._config.aws_role_arn and self._config.aws_role_session_name:
            try:
                assumed_role_response = self.session.client("sts").assume_role(
                    RoleArn=self._config.aws_role_arn,
                    RoleSessionName=self._config.aws_role_session_name
                )

                if assumed_role_response.Credentials:
                    self.session = Session(
                        aws_access_key_id=assumed_role_response.Credentials.AccessKeyId,
                        aws_secret_access_key=assumed_role_response.Credentials.SecretAccessKey,
                        aws_session_token=assumed_role_response.Credentials.SessionToken,
                        region_name=self._config.aws_region
                    )
            except Exception:
                logger.debug(
                    "Error assuming AWS Role", exc_info=True)

    def get_client(self, service_name: str) -> BaseClient:
        return self.session.client(service_name, endpoint_url=self._config.endpoint_url)

    def get_account_id(self):
        if self._config.aws_account_id:
            return self._config.aws_account_id
        else:
            return self.session.client("sts").get_caller_identity()["Account"]

    def get_region(self):
        return self._config.aws_region
