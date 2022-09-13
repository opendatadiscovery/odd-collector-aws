from odd_collector_aws.adapters.s3.strategies import EachFileStrategy, FolderStrategy
from odd_collector_aws.domain.dataset_config import DatasetConfig


def select_strategy(dataset_config: DatasetConfig):
    return FolderStrategy if dataset_config.folder_as_dataset else EachFileStrategy
