from typing import Dict, List, Any, Type
import requests
from oddrn_generator import Generator, MssqlGenerator
from json import loads
import copy


class SelectionMappingRule:
    def __init__(self, rules_node: Dict[str, Any]):
        self.rule_id: str = rules_node['rule-id']

        object_locator = rules_node['object-locator']

        self.schema_name: str = None if object_locator['schema-name'] == '%' else object_locator['schema-name']
        self.table_name: str = None if object_locator['table-name'] == '%' else object_locator['table-name']
        self.include: bool = False if rules_node['rule-action'] == 'exclude' else True


"""
            "schema-name": "test_schema",
            "table-name": "test_table"

"""

r = [
    {
        "rule-type": "selection",
        "rule-id": "814375997",
        "rule-name": "814375997",
        "object-locator": {
            "schema-name": "%",
            "table-name": "test_table"
        },
        "rule-action": "include",
        "filters": []
    },
    # {
    #     "rule-type": "selection",
    #     "rule-id": "856276228",
    #     "rule-name": "856276228",
    #     "object-locator": {
    #         "schema-name": "%",
    #         "table-name": "another_table"
    #     },
    #     "rule-action": "include",
    #     "filters": []
    # }
]


class EntitiesExtractor:
    def __init__(self, rules_nodes: List[Dict[str, Any]],
                 platform_host_url: str, oddrn_generator: Generator, deg_path_name: str):
        self.deg_path_name = deg_path_name
        self.oddrn_generator = oddrn_generator
        self.platform_host_url = platform_host_url
        self.rules_nodes = rules_nodes

    def request_items_by_deg(self, deg_oddrn: str) -> List[Dict[str, str]]:
        url = f'{self.platform_host_url}/ingestion/dataentities'
        params = {"deg_oddrn": deg_oddrn}
        resp = requests.get(url=url, params=params)
        return loads(resp.content)['items']

    def extract_tables_oddrns_from_items(self, deg_oddrn: str, accum_list: List[str]) -> List[str]:
        items_from_request = self.request_items_by_deg(deg_oddrn)
        if len(items_from_request) == 0:
            return []
        for item in items_from_request:
            entity_type = item['type']
            if entity_type == 'TABLE':
                accum_list.append(item['oddrn'])
            else:
                if entity_type == 'DATABASE_SERVICE':
                    self.extract_tables_oddrns_from_items(deg_oddrn=item['oddrn'], accum_list=accum_list)
                else:
                    raise NotImplementedError("not implemented entity type yet")

        return accum_list

    def create_selection_rules_list(self) -> List[SelectionMappingRule]:
        return [SelectionMappingRule(rule_node) for rule_node in self.rules_nodes if
                rule_node["rule-type"] == 'selection']

    def one_schema_one_table(self, schema_name: str, table_name: str) -> List[str]:
        """
        returns one table from one schema
        """
        gen = copy.copy(self.oddrn_generator)
        gen.set_oddrn_paths(**{self.deg_path_name: schema_name, "tables": table_name})
        table_oddrn = gen.get_oddrn_by_path("tables")
        schema_oddrn = gen.get_oddrn_by_path(self.deg_path_name)
        accum = []
        tables_oddrns_in_platform = self.extract_tables_oddrns_from_items(schema_oddrn, accum)
        if table_oddrn in tables_oddrns_in_platform:
            return [table_oddrn]
        return []

    def one_schema_all_tables(self, schema_name: str) -> List[str]:
        """
        returns all tables from one schema

        """
        gen = copy.copy(self.oddrn_generator)
        gen.set_oddrn_paths(**{self.deg_path_name: schema_name})
        schema_oddrn = gen.get_oddrn_by_path(self.deg_path_name)
        accum = []
        tables_oddrns_in_platform = self.extract_tables_oddrns_from_items(schema_oddrn, accum)
        return tables_oddrns_in_platform

    def all_schemas_one_table(self, table_name: str) -> List[str]:
        """
        returns one table with equal name from all schemas
        """
        tables_in_platform = self.all()
        gen = copy.copy(self.oddrn_generator)
        db_deg_oddrn = gen.get_data_source_oddrn()
        items = self.request_items_by_deg(db_deg_oddrn)
        schemas_oddrns_in_platform: List[str] = []
        for item in items:
            if item['type'] == 'DATABASE_SERVICE':
                schemas_oddrns_in_platform.append(item['oddrn'])
            else:
                raise NotImplementedError("not database")

        tables_oddrns_to_return: List[str] = []
        for schema_oddrn in schemas_oddrns_in_platform:
            table_oddrn = f'{schema_oddrn}/tables/{table_name}'
            if table_oddrn in tables_in_platform:
                tables_oddrns_to_return.append(table_oddrn)

        return tables_oddrns_to_return

    def all(self) -> List[str]:
        """

        returns all tables from all schemas
        """
        gen = copy.copy(self.oddrn_generator)
        db_deg_oddrn = gen.get_data_source_oddrn()
        accum = []
        tables_oddrns_in_platform = self.extract_tables_oddrns_from_items(db_deg_oddrn, accum)
        return tables_oddrns_in_platform

    def find_case(self, rule: SelectionMappingRule):
        if rule.include:
            if rule.schema_name is not None:
                if rule.table_name is not None:
                    return self.one_schema_one_table(rule.schema_name, rule.table_name)
                else:
                    return self.one_schema_all_tables(rule.schema_name)
            else:
                if rule.table_name is not None:
                    return self.all_schemas_one_table(rule.table_name)
                else:
                    return self.all()
        else:
            raise NotImplementedError('implement EXCLUDE case')

    def run(self):
        rules_list = self.create_selection_rules_list()
        for rule in rules_list:
            oddrn = self.find_case(rule)
            print(oddrn)


ggen = MssqlGenerator(host_settings='database-1.cs2rwctroxbf.eu-west-2.rds.amazonaws.com', databases='test_db')
ee = EntitiesExtractor(rules_nodes=r,
                       platform_host_url='http://localhost:8080', oddrn_generator=ggen,
                       deg_path_name='schemas')

ee.run()
pass
