from abc import ABC, abstractmethod

from odd_collector_aws.adapters.s3.clients.s3_client_base import S3ClientBase


class FetchStrategyBase(ABC):
    def __init__(self, s3_client: S3ClientBase):
        self.s3_client = s3_client

    @abstractmethod
    def get_datasets(self, dataset_config):
        raise NotImplementedError
