from typing import Union

from pyarrow._fs import FileInfo, FileSelector
from pyarrow.fs import S3FileSystem

from odd_collector_aws.domain.plugin import S3DeltaPlugin, S3Plugin

S3Storage = Union[S3Plugin, S3DeltaPlugin]


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, config: S3Storage):
        params = {
            "access_key": config.aws_access_key_id,
            "secret_key": config.aws_secret_access_key,
            "session_token": config.aws_session_token,
            "region": config.aws_region,
            "endpoint_override": config.endpoint_url,
            "role_arn": config.aws_role_arn,
            "session_name": config.aws_role_session_name,
        }

        self.fs = S3FileSystem(**params)

    def get_file_info(self, path: str) -> list[FileInfo]:
        """
        Get file info from path.
        @param path: s3 path to file or folder
        @return: FileInfo
        """
        return self.fs.get_file_info(FileSelector(base_dir=path))
