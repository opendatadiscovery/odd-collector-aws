from abc import ABC
from typing import Dict, Type

import pyarrow.dataset as ds

from odd_collector_aws.adapters.s3.errors import InvalidFileFormatError
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.adapters.s3.metadata_extractor import (
    FolderMetadataExtractor,
    FileMetadataExtractor,
)
from odd_collector_aws.utils import get_file_extension


class S3Dataset(ABC):
    """
    Adapter between pyarrow.Dataset and DataEntity
    """

    format = None

    def __init__(
        self,
        dataset: ds.Dataset,
        path: str,
        metadata: Dict[str, str],
        partitioning: str = None,
    ) -> None:
        self._dataset = dataset
        self._path = path
        self._partitioning = (
            ds.partitioning(flavor="hive") if partitioning else partitioning
        )
        self._metadata = metadata or {}

    @property
    def schema(self):
        return self._dataset.schema

    @property
    def path(self):
        return self._path

    @property
    def rows_number(self):
        return 0

    @property
    def metadata(self):
        return self._metadata

    def add_metadata(self, metadata: Dict[str, str]):
        self._metadata.update(metadata)


class CSVS3Dataset(S3Dataset):
    format = "csv"


class TSVS3Dataset(S3Dataset):
    format = "tsv"


class ParquetS3Dataset(S3Dataset):
    format = "parquet"


DATASETS: Dict[str, Type[S3Dataset]] = {
    ".csv": CSVS3Dataset,
    ".csv.gz": CSVS3Dataset,
    ".tsv": TSVS3Dataset,
    ".tsv.gz": TSVS3Dataset,
    ".parquet": ParquetS3Dataset,
}


def available_formats():
    return ", ".join(DATASETS.keys())


def create_s3_dataset(
    file_path: str, fs: FileSystem, original_path: str = None, partitioning: str = None
) -> S3Dataset:
    file_ext = get_file_extension(file_path)

    if file_ext not in DATASETS:
        raise InvalidFileFormatError(
            f"Got {file_path}, available format is {available_formats()}"
        )

    # Getting file or last file dataset
    dataset = fs.get_dataset(file_path, DATASETS[file_ext].format)
    path = original_path or file_path

    if path.endswith("/"):
        metadata = FolderMetadataExtractor(path, dataset, fs, partitioning).extract()
    else:
        metadata = FileMetadataExtractor(path, dataset, fs).extract()

    s3_dataset = DATASETS[file_ext](
        dataset,
        path,
        metadata,
        partitioning,
    )

    return s3_dataset
