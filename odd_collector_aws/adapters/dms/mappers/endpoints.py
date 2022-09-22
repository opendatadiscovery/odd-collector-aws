from oddrn_generator.generators import MssqlGenerator
from typing import Dict, Any, Union, Type, List
from odd_models.models import DataEntity, DataEntityType, DataEntityGroup
from oddrn_generator.server_models import AWSCloudModel, AbstractServerModel
from abc import abstractmethod


class EndpointEngine:
    def __init__(self, endpoint_node: Dict[str, Any],
                 server_obj: Union[AWSCloudModel, AbstractServerModel]):
        self.server_obj = server_obj
        self.stats = endpoint_node.get(self.settings_node_name)

    engine_name: str
    settings_node_name: str

    @abstractmethod
    def get_database_oddrn(self) -> str:
        pass

    @abstractmethod
    def map_database(self) -> DataEntity:
        pass

    def map_table(self, table_name: str):
        return DataEntity(name=table_name,
                          oddrn=f"{self.get_database_oddrn()}/tables/{table_name}",
                          type=DataEntityType.TABLE,
                          )

    def map_database_with_tables(self, tables: List[str]) -> DataEntity:
        database = self.map_database()
        database.data_entity_group = DataEntityGroup(
            entities_list=[]
        )


class MssqlEngine(EndpointEngine):
    engine_name = 'sqlserver'
    settings_node_name = 'MicrosoftSQLServerSettings'

    def get_database_oddrn(self) -> str:
        gen = MssqlGenerator(
            host_settings=f"{self.stats['ServerName']}:{self.stats['Port']}", databases=self.stats['DatabaseName']
        )
        return gen.get_data_source_oddrn()

    def map_database(self) -> DataEntity:
        return DataEntity(name=self.stats['DatabaseName'],
                          oddrn=self.get_database_oddrn(),
                          type=DataEntityType.DATABASE_SERVICE,
                          )


class S3Engine(EndpointEngine):
    engine_name = 's3'
    settings_node_name = 'S3Settings'

    def get_database_oddrn(self) -> str:
        return (
            "//s3/cloud/aws"
            f"/account/{self.server_obj.account}"
            f"/region/{self.server_obj.region}"
            f"/bucket/{self.stats.get('BucketName')}"
            f"/folder/{self.stats.get('BucketFolder')}"
        )

    def map_database(self) -> DataEntity:
        return DataEntity(name=self.stats.get('BucketFolder'),
                          oddrn=self.get_database_oddrn(),
                          type=DataEntityType.FILE
                          )


engines: List[Type[EndpointEngine]] = [
    MssqlEngine,
    S3Engine
]
engines_map: Dict[str, Type[EndpointEngine]] = {
    engine.engine_name: engine for engine in engines

}
