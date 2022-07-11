from typing import List

from odd_collector_aws.adapters.s3.clients.s3_client_base import S3ClientBase
from odd_collector_aws.adapters.s3.domain.dataset import S3Dataset
from odd_collector_aws.domain.plugin import DatasetConfig
from odd_collector_aws.adapters.s3.strategies.select_strategy import select_strategy


class S3DatasetService:
    s3_client: S3ClientBase

    def __init__(self, s3_client: S3ClientBase):
        self.s3_client = s3_client

    def get_datasets(self, dataset_config: DatasetConfig) -> List[S3Dataset]:
        strategy = select_strategy(dataset_config)(self.s3_client)

        return strategy.get_datasets(dataset_config)
