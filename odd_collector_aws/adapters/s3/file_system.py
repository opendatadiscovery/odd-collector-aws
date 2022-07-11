import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, config):
        self.__fs = S3FileSystem(
            access_key=config.aws_access_key_id,
            secret_key=config.aws_secret_access_key,
            region=config.aws_region,
            endpoint_override=config.endpoint_url,
        )

    def get_dataset(self, file_path: str, format: str) -> ds.Dataset:
        return ds.dataset(file_path, filesystem=self.__fs, format=format)

    def get_folder_dataset(
        self, folder_path: str, format: str, partitioning: str = None
    ) -> ds.Dataset:
        return ds.dataset(
            folder_path, filesystem=self.__fs, format=format, partitioning=partitioning
        )
