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

    @abstractmethod
    def get_oddrn_for_schema_name(self, generator: Generator, schema_name: str) -> str:
        pass

    @abstractmethod
    def get_oddrn_for_table_schema_names(self, generator: Generator, schema_name: str, table_name: str) -> str:
        pass

    @abstractmethod
    def extend_schema_oddrn_with_table_name(self, schema_oddrn: str, table_name: str) -> str:
        pass


class JdbcEngine(EndpointEngine):
    @abstractmethod
    def get_generator(self) -> Generator:
        pass

    def get_oddrn_for_schema_name(self, generator: Generator, schema_name: str) -> str:
        generator.set_oddrn_paths(**{self.schemas_path_name: schema_name})
        return generator.get_oddrn_by_path(self.schemas_path_name)

    def get_oddrn_for_table_schema_names(self, generator: Generator, schema_name: str, table_name: str) -> str:
        generator.set_oddrn_paths(**{self.schemas_path_name: schema_name, self.tables_path_name: table_name})
        return generator.get_oddrn_by_path(self.tables_path_name)

    def extend_schema_oddrn_with_table_name(self, schema_oddrn: str, table_name: str) -> str:
        return f"{schema_oddrn}/{self.tables_path_name}/{table_name}"


class MssqlEngine(JdbcEngine):
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
