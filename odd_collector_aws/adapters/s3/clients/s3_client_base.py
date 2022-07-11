from abc import ABC, abstractmethod
from typing import List, Dict, Any

from odd_collector_aws.adapters.s3.file_system import FileSystem

S3Object = Dict[Any, Any]


class S3ClientBase(ABC):
    fs: FileSystem

    @abstractmethod
    def get_list_files(self, bucket: str, prefix: str) -> List[S3Object]:
        raise NotImplementedError

    @abstractmethod
    def get_last_modified_file(self, bucket: str, prefix: str) -> S3Object:
        raise NotImplementedError
