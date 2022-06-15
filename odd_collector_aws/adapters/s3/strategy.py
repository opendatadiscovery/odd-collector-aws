from typing import List


from odd_collector_aws.adapters.s3.errors import EmptyFolderError
from odd_collector_aws.adapters.s3.client import Client
from odd_collector_aws.adapters.s3.dataset import S3Dataset, create_s3_dataset
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.domain.plugin import DatasetConfig


def file_strategy(dataset_config: DatasetConfig, fs: FileSystem) -> List[S3Dataset]:
    dataset = create_s3_dataset(dataset_config.path, fs)
    return [dataset]


def each_file_strategy(
    ds_config: DatasetConfig, s3_client: Client, fs: FileSystem
) -> List[S3Dataset]:
    s3_objects = s3_client.list_objects(bucket=ds_config.bucket, prefix=ds_config.path)

    paths = [ds_config.bucket + "/" + e.get("Key") for e in s3_objects]

    datasets = []
    for file_path in paths:
        dataset = create_s3_dataset(file_path, fs)
        datasets.append(dataset)

    return datasets


def folder_strategy(
    dataset_config: DatasetConfig,
    s3_client: Client,
    fs: FileSystem,
) -> List[S3Dataset]:
    """
    Strategy for folders. It will find the last file in the folder and use it as the dataset.
    We send dataset_config.path as the original_path to the dataset.
    """
    list_objects = s3_client.list_objects(
        bucket=dataset_config.bucket, prefix=dataset_config.path
    )

    last_modified = find_last_file(list_objects)
    s3_dataset = create_s3_dataset(
        dataset_config.bucket + "/" + last_modified.get("Key"),
        fs,
        dataset_config.full_path,
        dataset_config.partitioning,
    )
    return [s3_dataset]


def find_last_file(s3_objects):
    """Find last modified file in a folder"""
    if not s3_objects:
        raise EmptyFolderError("s3_objects is empty or None")
    return sorted(s3_objects, key=lambda x: x.get("LastModified"), reverse=True)[0]
