from functools import singledispatchmethod
from typing import List

from odd_collector_aws.adapters.s3.s3_dataset_service import S3DatasetService
from odd_collector_aws.adapters.s3.domain.dataset import S3Dataset
from odd_collector_aws.domain.plugin import DatasetConfig
from odd_collector_aws.utils.parse_s3_url import parse_s3_url


class S3DatasetUseCase:
    client: S3DatasetService

    def __init__(self, s3_dataset_client: S3DatasetService):
        self.client = s3_dataset_client

    @singledispatchmethod
    def get_datasets(self, arg) -> List[S3Dataset]:
        """
        @param arg: Union[str, DatasetConfig]
            If argument is str, try to parse it as s3 url and create DatasetConfig
            If argument is DatasetConfig use it as is
        @rtype: List[S3Dataset]
        """
        raise NotImplementedError

    @get_datasets.register
    def _(self, arg: str) -> List[S3Dataset]:
        bucket, path = parse_s3_url(arg)

        ds_config = DatasetConfig(bucket=bucket, path=path)

        return self.get_datasets(ds_config)

    @get_datasets.register
    def _(self, arg: DatasetConfig) -> List[S3Dataset]:
        return self.client.get_datasets(arg)
