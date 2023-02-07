from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import AWSCloudModel
from oddrn_generator import Generator
from odd_collector_aws.aws.aws_client import AwsClient
from typing import Optional, List, Dict, Any
from odd_models.models import DataEntityList, DataEntity, DataEntityType, DataEntityGroup
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector_aws.domain.plugin import EfsPlugin, AwsPlugin


class EfsPathModel(BasePathsModel):
    file_systems: Optional[str]

    class Config:
        dependencies_map = {"file_systems": ("file_systems",)}


class EfsGenerator(Generator):
    source = "efs"
    paths_model = EfsPathModel
    server_model = AWSCloudModel


class EfsClient:
    def __init__(self, config: AwsPlugin):
        self._config = config

        self.efs = AwsClient(config).get_client("efs")
        self.account_id = AwsClient(config).get_account_id()

    def get_file_systems(self) -> List[Dict[str, Any]]:
        return self.efs.describe_file_systems()['FileSystems']


class Adapter(AbstractAdapter):
    def __init__(self, config: EfsPlugin) -> None:
        self._efs_client = EfsClient(config)
        self._oddrn_generator = EfsGenerator(
            cloud_settings={
                "region": config.aws_region,
                "account": self._efs_client.account_id,
            }
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def _map_filesystems(self, filesystem_node: Dict[str, Any]) -> DataEntity:
        return DataEntity(
            oddrn=self._oddrn_generator.get_oddrn_by_path("file_systems", filesystem_node.get('FileSystemId')),
            name=filesystem_node.get('Name', filesystem_node.get('FileSystemId')),
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[]
            ),
        )

    def get_data_entity_list(self) -> DataEntityList:
        filesystems = self._efs_client.get_file_systems()
        fs_entities: List[DataEntity] = []
        for fs in filesystems:
            fs_entities.append(self._map_filesystems(fs))

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*fs_entities],
        )
