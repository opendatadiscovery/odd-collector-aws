from boto3 import Session
from botocore.client import BaseClient

from odd_collector_aws.domain.plugin import AwsPlugin


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

    def get_client(self, service_name: str) -> BaseClient:
        return self.session.client(service_name, endpoint_url=self._config.endpoint_url)

    def get_account_id(self):
        if self._config.aws_account_id:
            return self._config.aws_account_id
        else:
            return self.session.client("sts").get_caller_identity()["Account"]

    def get_region(self):
        return self._config.aws_region
