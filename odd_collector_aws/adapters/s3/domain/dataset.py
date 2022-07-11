from typing import Dict, Type

import pyarrow.dataset as ds
from oddrn_generator.generators import S3Generator

from odd_collector_aws.adapters.s3.mapper.dataset import map_dataset
from odd_collector_aws.domain.to_data_entity import ToDataEntity
from odd_collector_aws.utils import parse_s3_url


class S3Dataset(ToDataEntity):
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
    def bucket(self):
        if self._path.startswith("s3://"):
            bucket, _ = parse_s3_url(self._path)
        else:
            return self._path.split("/")[0]

    @property
    def arn(self):
        return self.path

    @property
    def schema(self):
        return self._dataset.schema

    @property
    def path(self):
        return self._path

    @property
    def rows_number(self):
        return self._dataset.count_rows()

    @property
    def metadata(self):
        return self._metadata

    def add_metadata(self, metadata: Dict[str, str]):
        self._metadata.update(metadata)

    def to_data_entity(self, oddrn_generator: S3Generator):
        return map_dataset(self, oddrn_generator)


class CSVS3Dataset(S3Dataset):
    format = "csv"


class TSVS3Dataset(S3Dataset):
    format = "tsv"


class ParquetS3Dataset(S3Dataset):
    format = "parquet"


DATASETS_FN: Dict[str, Type[S3Dataset]] = {
    ".csv": CSVS3Dataset,
    ".csv.gz": CSVS3Dataset,
    ".tsv": TSVS3Dataset,
    ".tsv.gz": TSVS3Dataset,
    ".parquet": ParquetS3Dataset,
}

AVAILABLE_FILE_FORMATS = ", ".join(DATASETS_FN.keys())
