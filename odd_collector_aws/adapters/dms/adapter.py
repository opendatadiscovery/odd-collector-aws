from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.aws.aws_client import AwsClient
from typing import Optional, Iterable, Dict, Any
from odd_collector_sdk.domain.adapter import AbstractAdapter
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.generators import Generator
from oddrn_generator.server_models import AWSCloudModel
from odd_collector_aws.domain.plugin import DmsPlugin
from itertools import chain
from odd_collector_aws.adapters.dms.mappers.endpoints import engines_map
from odd_models.models import DataEntityList, DataEntity, DataEntityType, DataTransformer
from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.domain.fetch_paginator import fetch_paginator

MAX_RESULTS_FOR_PAGE = 100


class DmsPathsModel(BasePathsModel):
    tasks: Optional[str]
    runs: Optional[str]

    class Config:
        dependencies_map = {
            "tasks": ("tasks",),
            "runs": ("tasks", "runs"),
        }


class DmsGenerator(Generator):
    source = "dms"
    paths_model = DmsPathsModel
    server_model = AWSCloudModel


def map_dms_task(
        raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    endpoints_arn_dict: Dict[str, DataEntity] = mapper_args["endpoints_arn_dict"]
    trans = DataTransformer(
        inputs=[endpoints_arn_dict.get(raw_job_data.get('SourceEndpointArn')).oddrn],

        outputs=[endpoints_arn_dict.get(raw_job_data.get('TargetEndpointArn')).oddrn],

    )
    data_entity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tasks", raw_job_data['ReplicationTaskIdentifier']),
        name=raw_job_data['ReplicationTaskIdentifier'],
        owner=None,
        type=DataEntityType.JOB,
    )
    data_entity.data_transformer = trans
    return data_entity


#


class DMSClient:

    def __init__(self, config: AwsPlugin):
        self._config = config

        self.dms = AwsClient(config).get_client("dms")
        self.account_id = AwsClient(config).get_account_id()


class Adapter(AbstractAdapter):
    def __init__(self, config: DmsPlugin) -> None:
        self._dms_client = DMSClient(config)
        self._oddrn_generator = DmsGenerator(
            cloud_settings={"region": config.aws_region, "account": self._dms_client.account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        endpoints_entities_dict = self._get_endpoints_entities_arn_dict()
        tasks_entities = list(chain(
            self._get_tasks(endpoints_entities_dict),
        ))
        endpoints_entities_values = list(endpoints_entities_dict.values())

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tasks_entities, *endpoints_entities_values],
        )

    def _get_tasks(self, endpoints_entities_arn_dict: Dict[str, DataEntity]) -> Iterable[DataEntity]:
        return fetch_paginator(
            PaginatorConfig(
                op_name="describe_replication_tasks",
                parameters={},
                page_size=MAX_RESULTS_FOR_PAGE,
                list_fetch_key='ReplicationTasks',
                mapper=map_dms_task,
                mapper_args={"oddrn_generator": self._oddrn_generator,
                             "endpoints_arn_dict": endpoints_entities_arn_dict,
                             },
            ),
            self._dms_client.dms
        )

    def _get_endpoints_nodes(self):
        paginator = fetch_paginator(
            PaginatorConfig(
                op_name="describe_endpoints",
                parameters={},
                page_size=MAX_RESULTS_FOR_PAGE,
                list_fetch_key='Endpoints',
            ),
            self._dms_client.dms
        )
        return paginator

    def _get_endpoints_entities_arn_dict(self):
        return {endpoint_node.get('EndpointArn'): engines_map.get(endpoint_node.get('EngineName'))(endpoint_node,
                                                                                                   self._oddrn_generator.server_obj).map_data_entity()
                for endpoint_node in self._get_endpoints_nodes()}
