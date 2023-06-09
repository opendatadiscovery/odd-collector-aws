from pyarrow._fs import FileInfo, FileSelector
from pyarrow.fs import S3FileSystem

from odd_collector_aws.domain.plugin import AwsPlugin


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
