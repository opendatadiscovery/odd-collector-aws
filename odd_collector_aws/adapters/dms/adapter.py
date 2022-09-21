from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.aws.aws_client import AwsClient
from typing import Optional, Iterable, Dict, Any
from odd_collector_sdk.domain.adapter import AbstractAdapter
from os import getenv
from odd_collector_aws.domain.dataset_config import DatasetConfig
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.generators import Generator
from oddrn_generator.server_models import AWSCloudModel, AbstractServerModel
from odd_collector_aws.domain.plugin import DmsPlugin
from itertools import chain
from odd_models.models import DataEntityList, DataEntity, DataEntityType, DataTransformer
from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.domain.fetch_paginator import fetch_paginator
from oddrn_generator.generators import MssqlGenerator, S3Generator

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
    endpoints_arn_dict: Dict[str, Dict[str, Any]] = mapper_args["endpoints_arn_dict"]
    trans = DataTransformer(
        inputs=[get_endpoint_oddrn(endpoints_arn_dict.get(raw_job_data.get('SourceEndpointArn')),
                                   oddrn_generator.server_obj)
                ],
        outputs=[get_endpoint_oddrn(endpoints_arn_dict.get(raw_job_data.get('TargetEndpointArn')),
                                    oddrn_generator.server_obj)],
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
def get_endpoint_oddrn(endpoint_node: Dict[str, Any], server_obj: AWSCloudModel) -> str:
    engine_name = endpoint_node.get('EngineName')
    if engine_name == 'sqlserver':
        stats = endpoint_node.get('MicrosoftSQLServerSettings')
        gen = MssqlGenerator(
            host_settings=f"{stats['ServerName']}:{stats['Port']}", databases=stats['DatabaseName']
        )
    elif engine_name == 's3':
        gen = S3Generator(
            cloud_settings={"region": server_obj.region, "account": server_obj.account}
        )
    else:
        return 'hello'
    return gen.get_data_source_oddrn()


class DMSClient:

    def __init__(self, config: AwsPlugin):
        self._config = config

        self.dms = AwsClient(config).get_client("dms")
        self.account_id = AwsClient(config).get_account_id()

    # def get_endpoint_node(self, endpoint_arn: str) -> dict:
    #     response = self.dms.describe_endpoints(Filters=[
    #         {
    #             'Name': 'endpoint-arn',
    #             'Values': [
    #                 endpoint_arn,
    #             ]
    #         },
    #     ],
    #         Marker='string')
    #     return response['Endpoints'][0]


_config = DmsPlugin(aws_secret_access_key=getenv('aws_secret_access_key'),
                    aws_access_key_id=getenv('aws_access_key_id'),
                    aws_region='eu-west-2',
                    name='dms_name',
                    type='dms'
                    )


class Adapter(AbstractAdapter):
    def __init__(self, config: DmsPlugin) -> None:
        self._dms_client = DMSClient(config)
        self._oddrn_generator = DmsGenerator(
            cloud_settings={"region": config.aws_region, "account": self._dms_client.account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        items = chain(
            self._get_tasks(),
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(items),
        )

    def _get_tasks(self) -> Iterable[DataEntity]:
        return fetch_paginator(
            PaginatorConfig(
                op_name="describe_replication_tasks",
                parameters={},
                page_size=MAX_RESULTS_FOR_PAGE,
                list_fetch_key='ReplicationTasks',
                mapper=map_dms_task,
                mapper_args={"oddrn_generator": self._oddrn_generator,
                             "endpoints_arn_dict": self._get_endpoints_arn_dict(),
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

    def _get_endpoints_arn_dict(self):
        return {endpoint_node.get('EndpointArn'): endpoint_node for endpoint_node in self._get_endpoints_nodes()}


dms_client = DMSClient(_config)

# tasks_nodes = dms_client.get_replication_tasks_nodes()
# endp_node = dms_client.get_endpoint_node(
#     'arn:aws:dms:eu-west-2:245260513500:endpoint:7DRJ5QSHOPBBCAYW76EA6SHZXMBCCV67XHPML4Y')


ad = Adapter(_config)
# print(ad.get_data_source_oddrn())
# tasks = ad._dms_client.get_replication_tasks_nodes()
# en_list = ad._get_endpoints_nodes()
# en_arn_list = ad._get_endpoints_arn_dict()
ent = ad.get_data_entity_list()

pass
