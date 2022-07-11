from typing import List, Any

from odd_collector_aws.adapters.s3.domain.dataset import (
    DATASETS_FN,
    AVAILABLE_FILE_FORMATS,
)
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.adapters.s3.mapper.metadata_extractor import (
    FileMetadataExtractor,
    FolderMetadataExtractor,
)
from odd_collector_aws.adapters.s3.strategies.fetch_strategy_base import (
    FetchStrategyBase,
)
from odd_collector_aws.domain.dataset_config import DatasetConfig
from odd_collector_aws.errors import InvalidFileFormatWarning
from odd_collector_aws.utils import get_file_extension
from odd_collector_aws.adapters.s3.logger import logger


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
                logger.info(f"Create dataset for {file_path}")
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
    file_ext = get_file_extension(file_path)

    validate_file_extension(file_ext)

    dataset = fs.get_dataset(file_path, DATASETS_FN[file_ext].format)
    metadata = FileMetadataExtractor(file_path, dataset, fs).extract()

    return DATASETS_FN[file_ext](
        dataset,
        file_path,
        metadata,
    )


def create_s3_dataset_for_folder(
    last_file_path: str, folder_path: str, fs: FileSystem, partitioning
):
    file_ext = get_file_extension(last_file_path)

    validate_file_extension(file_ext)

    dataset = fs.get_dataset(last_file_path, DATASETS_FN[file_ext].format)
    metadata = FolderMetadataExtractor(
        last_file_path, dataset, fs, partitioning
    ).extract()

    return DATASETS_FN[file_ext](dataset, folder_path, metadata, partitioning)


def validate_file_extension(file_ext: str):
    if file_ext not in DATASETS_FN:
        raise InvalidFileFormatWarning(
            f"Got {file_ext}, available formats is {AVAILABLE_FILE_FORMATS}"
        )
