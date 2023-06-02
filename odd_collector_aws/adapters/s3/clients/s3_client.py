from odd_collector_aws.adapters.s3.clients.s3_client_base import S3ClientBase, S3Object
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.aws.aws_client import AwsClient
from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.errors import EmptyFolderError


class S3Client(S3ClientBase):
    """
    @deprecated
    Client hides boto3 implementation details.
    """

    def __init__(self, config: AwsPlugin):
        self._config = config

        self.s3 = AwsClient(config).get_client("s3")
        self.fs = FileSystem(config)
        self.s3_folders = {}

    def get_list_files(self, bucket: str, prefix: str) -> list[S3Object]:
        """
        List file objects in a bucket. Minio doesn't return folders as objects.
        """
        try:
            objects = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix.lstrip("/"))[
                "Contents"
            ]

            self.s3_folders[prefix] = [
                e["Key"]
                for e in objects
                if not e.get("Size") and e["Key"].endswith("/")
            ]
        except KeyError as e:
            raise EmptyFolderError(f"{bucket}/{prefix}") from e
        else:
            return [e for e in objects if e.get("Size")]

    def get_last_modified_file(self, bucket: str, prefix: str) -> S3Object:
        list_objects = self.get_list_files(bucket=bucket, prefix=prefix)

        return self._find_last_file(list_objects)

    @staticmethod
    def _find_last_file(s3_objects):
        """Find last modified file in a folder"""
        return sorted(s3_objects, key=lambda x: x.get("LastModified"), reverse=True)[0]
