from oddrn_generator.generators import MssqlGenerator, Generator, S3Generator
from typing import Dict, Any, Type, List
from abc import abstractmethod


class EndpointEngine:
    def __init__(self, endpoint_node: Dict[str, Any]):
        self.stats = endpoint_node.get(self.settings_node_name)

    engine_name: str
    settings_node_name: str
    schemas_path_name: str
    tables_path_name: str

    @abstractmethod
    def get_generator(self) -> Generator:
        pass


class MssqlEngine(EndpointEngine):
    engine_name = "sqlserver"
    settings_node_name = "MicrosoftSQLServerSettings"
    schemas_path_name = 'schemas'
    tables_path_name = 'tables'

    def get_generator(self) -> Generator:
        return MssqlGenerator(
            host_settings=f"{self.stats['ServerName']}",
            databases=self.stats["DatabaseName"],
        )


class S3Engine(EndpointEngine):
    engine_name = "s3"
    settings_node_name = "S3Settings"
    schemas_path_name = 'buckets'
    tables_path_name = 'keys'

    def get_generator(self) -> Generator:
        return S3Generator()


engines: List[Type[EndpointEngine]] = [MssqlEngine]
engines_factory: Dict[str, Type[EndpointEngine]] = {
    engine.engine_name: engine for engine in engines
}
