from typing import Any, Protocol

from odd_collector_aws.adapters.s3.file_system import FileSystem

S3Object = dict[Any, Any]


class S3ClientBase(Protocol):
    fs: FileSystem

    def get_list_files(self, bucket: str, prefix: str) -> list[S3Object]:
        ...

    def get_last_modified_file(self, bucket: str, prefix: str) -> S3Object:
        ...
