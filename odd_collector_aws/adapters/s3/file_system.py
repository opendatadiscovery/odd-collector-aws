from typing import Union

import pyarrow.dataset as ds
from funcy import iffy, lmap
from pyarrow._fs import FileInfo, FileSelector
from pyarrow.fs import S3FileSystem

from odd_collector_aws.domain.plugin import AwsPlugin

from ...domain.dataset_config import DatasetConfig
from .domain.models import Bucket, File, Folder
from .logger import logger
from .utils import file_format


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, config: AwsPlugin):
        params = {}

        if config.aws_access_key_id:
            params["access_key"] = config.aws_access_key_id
        if config.aws_secret_access_key:
            params["secret_key"] = config.aws_secret_access_key
        if config.aws_session_token:
            params["session_token"] = config.aws_session_token
        if config.aws_region:
            params["region"] = config.aws_region
        if config.endpoint_url:
            params["endpoint_override"] = config.endpoint_url

        self.fs = S3FileSystem(**params)

    def get_file_info(self, path: str) -> list[FileInfo]:
        """
        Get file info from path.
        @param path: s3 path to file or folder
        @return: FileInfo
        """
        return self.fs.get_file_info(FileSelector(base_dir=path))

    def get_dataset(self, file_path: str, format: str) -> ds.Dataset:
        """
        Get dataset from file path.
        @param file_path:
        @param format: Should be one of available formats: https://arrow.apache.org/docs/python/api/dataset.html#file-format
        @return: Dataset
        """
        return ds.dataset(source=file_path, filesystem=self.fs, format=format)

    def get_folder_as_file(self, dataset_config: DatasetConfig) -> File:
        """
        Get folder as Dataset.
        @param dataset_config:
        @return: File
        """
        logger.debug(f"Getting folder dataset for {dataset_config=}")

        dataset = ds.dataset(
            source=dataset_config.full_path,
            format=dataset_config.folder_as_dataset.file_format,
            partitioning=ds.partitioning(
                flavor=dataset_config.folder_as_dataset.flavor,
                field_names=dataset_config.folder_as_dataset.field_names,
            ),
            filesystem=self.fs,
        )
        return File(
            path=dataset_config.full_path,
            base_name=dataset_config.full_path,
            schema=dataset.schema,
            metadata={
                "Format": dataset_config.folder_as_dataset.file_format,
                "Partitioning": dataset_config.folder_as_dataset.flavor,
                "Flavor": dataset_config.folder_as_dataset.flavor,
                "FieldNames": dataset_config.folder_as_dataset.field_names,
            },
            format=dataset_config.folder_as_dataset.file_format,
        )

    def get_bucket(self, dataset_config: DatasetConfig) -> Bucket:
        """
        Get bucket with all related objects.
        @param dataset_config:
        @return: Bucket
        """
        bucket = Bucket(dataset_config.bucket)
        if dataset_config.folder_as_dataset:
            bucket.objects.append(self.get_folder_as_file(dataset_config))
        else:
            objects = self.list_objects(path=dataset_config.full_path)
            bucket.objects.extend(objects)

        return bucket

    def list_objects(self, path: str) -> list[Union[File, Folder]]:
        """
        Recursively get objects for path.
        @param path: s3 path
        @return: list of either File or Folder
        """
        logger.debug(f"Getting objects for {path=}")
        return lmap(
            iffy(
                lambda x: x.is_file,
                lambda x: self.get_file(x.path, x.base_name),
                lambda x: self.get_folder(x.path),
            ),
            self.get_file_info(path),
        )

    def get_file(self, path: str, file_name: str = None) -> File:
        """
        Get File with schema and metadata.
        @param path: s3 path to file
        @param file_name: file name
        @return: File
        """
        path = remove_protocol(path)
        if not file_name:
            file_name = path.split("/")[-1]

        try:
            file_fmt = file_format(file_name)
            dataset = self.get_dataset(path, file_fmt)

            return File.dataset(
                path=path,
                name=file_name,
                schema=dataset.schema,
                file_format=file_fmt,
                metadata={},
            )
        except Exception as e:
            logger.warning(f"Failed to get schema for file {path}: {e}")
            return File.unknown(
                path=path,
                base_name=file_name,
                file_format="unknown",
            )

    def get_folder(self, path: str, recursive: bool = True) -> Folder:
        """
        Get Folder with objects recursively.
        @param path: s3 path to
        @return: Folder class with objects and path
        """
        path = remove_protocol(path)
        objects = self.list_objects(path) if recursive else []
        return Folder(path, objects)


def remove_protocol(path: str) -> str:
    if path.startswith("s3://"):
        return path.removeprefix("s3://")
    elif path.startswith(("s3a://", "s3n://")):
        return path[6:]
    else:
        return path
