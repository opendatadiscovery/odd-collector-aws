from typing import Any, List

from odd_collector_aws.adapters.s3.domain.dataset import get_dataset_class
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.adapters.s3.logger import logger
from odd_collector_aws.adapters.s3.mapper.metadata_extractor import (
    FileMetadataExtractor,
    FolderMetadataExtractor,
)
from odd_collector_aws.adapters.s3.strategies.fetch_strategy_base import (
    FetchStrategyBase,
)
from odd_collector_aws.domain.dataset_config import DatasetConfig
from odd_collector_aws.errors import InvalidFileFormatWarning


class FileStrategy(FetchStrategyBase):
    def get_datasets(self, dataset_config) -> List[Any]:
        return [create_s3_dataset_for_file(dataset_config.path, self.s3_client.fs)]


class EachFileStrategy(FetchStrategyBase):
    def get_datasets(self, dataset_config) -> List[Any]:
        bucket = dataset_config.bucket
        prefix = dataset_config.path

        objects = self.s3_client.get_list_files(bucket, prefix)

        paths = [f"{bucket}/{obj.get('Key')}" for obj in objects]

        result = []
        for file_path in paths:
            try:
                logger.debug(f"Create dataset for {file_path}")
                result.append(create_s3_dataset_for_file(file_path, self.s3_client.fs))
            except InvalidFileFormatWarning as e:
                logger.warning(e)
                continue
        return result


class FolderStrategy(FetchStrategyBase):
    def get_datasets(self, dataset_config: DatasetConfig) -> List[Any]:
        bucket = dataset_config.bucket
        prefix = dataset_config.path

        last_modified = self.s3_client.get_last_modified_file(bucket, prefix)

        s3_dataset = create_s3_dataset_for_folder(
            last_file_path=f"{bucket}/{last_modified.get('Key').lstrip('/')}",
            folder_path=dataset_config.full_path,
            fs=self.s3_client.fs,
            partitioning=dataset_config.partitioning,
        )
        return [s3_dataset]


def create_s3_dataset_for_file(file_path: str, fs: FileSystem):
    dataset_class = get_dataset_class(file_path)
    file_format = dataset_class.get_format()
    dataset = fs.get_dataset(file_path, file_format)
    logger.debug("extract metadata")
    metadata = FileMetadataExtractor(file_path, dataset, fs).extract()
    logger.debug("Metadata extracted")
    return dataset_class(dataset, file_path, metadata)


def create_s3_dataset_for_folder(
    last_file_path: str, folder_path: str, fs: FileSystem, partitioning
):

    dataset_class = get_dataset_class(last_file_path)
    file_format = dataset_class.get_format()
    dataset = fs.get_dataset(last_file_path, file_format)
    metadata = FolderMetadataExtractor(
        last_file_path, dataset, fs, partitioning
    ).extract()

    return dataset_class(dataset, folder_path, metadata, partitioning)
