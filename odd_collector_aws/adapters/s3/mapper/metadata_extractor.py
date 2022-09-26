from abc import ABC, abstractmethod
from typing import Tuple

import pyarrow.dataset as ds

from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.adapters.s3.logger import logger
from odd_collector_aws.utils import parse_s3_url


class MetadataExtractor(ABC):
    def __init__(
        self,
        original_path: str,
        dataset: ds.Dataset,
        fs: FileSystem,
        partitioning: str = None,
    ) -> None:
        self._original_path = original_path
        self._dataset = dataset
        self._fs = fs
        self._partitioning = partitioning

    @abstractmethod
    def extract(self) -> dict:
        pass


class FolderMetadataExtractor(MetadataExtractor):
    N_FILES_SIZE_ESTIMATION = 50

    def extract(self) -> dict:
        file_dataset = self._dataset
        file_extension = file_dataset.format.default_extname
        bucket, key = split_to_bucket_key(self._original_path)
        result = {}
        folder_dataset = self._fs.get_folder_dataset(
            self._original_path,
            file_dataset.format.default_extname,
            partitioning=self._partitioning,
        )

        result["Format"] = file_extension
        result["Files"] = len(folder_dataset.files)
        result["Bucket"] = bucket
        result["Key"] = key
        result["Region"] = folder_dataset.filesystem.region
        li = []

        for file in folder_dataset.files[0 : self.N_FILES_SIZE_ESTIMATION]:
            li.append(folder_dataset.filesystem.get_file_info(file).size)

        result["Avg. file size"] = round(sum(li) / len(li))
        result["Estimated dataset size"] = self.average_size(
            result["Avg. file size"], result["Files"]
        )
        result["Partitioning"] = self._partitioning or "Default directory partitioning"
        return result

    @staticmethod
    def average_size(avg_file_size, n_files):
        return str(round(avg_file_size * n_files / 1024 / 1024 / 1024)) + " GB"


class FileMetadataExtractor(MetadataExtractor):
    def extract(self) -> dict:
        logger.info(f"Parse {self._original_path}")
        bucket, key = parse_s3_url(self._original_path)
        logger.info(f"Parsed {bucket}/{key}")

        logger.info(f"Count rows")
        rows = self._dataset.count_rows()
        logger.info(f"Counted rows {rows}")

        return {
            "Format": self._dataset.format.default_extname,
            "Rows": rows,
            "Bucket": bucket,
            "Key": key,
        }


def split_to_bucket_key(path: str) -> Tuple[str, str]:
    return path.split("/")[0], "/".join(path.split("/")[1:])
