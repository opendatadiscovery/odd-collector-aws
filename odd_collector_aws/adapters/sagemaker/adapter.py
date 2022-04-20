import itertools
from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import SageMakerGenerator

from odd_collector_aws.domain.plugin import SagemakerPlugin
from .client.sagemaker_client import SagemakerClient
from .mappers import map_experiment


class Adapter(AbstractAdapter):
    def __init__(self, config: SagemakerPlugin):
        self.config = config

        self.client = SagemakerClient(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )

        self.__oddrn_generator = SageMakerGenerator(
            cloud_settings={"region": config.aws_region, "account": self.client.account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self):
        # //sagemaker/cloud/aws/account/311638508164/region/us-east-1
        base_oddrn = self.get_data_source_oddrn()
        experiments = self.client.get_experiments(self.config.experiments)

        data_entities = (map_experiment(i, base_oddrn) for i in experiments)
        flatten: List[DataEntity] = list(itertools.chain(*data_entities))
        de_list = DataEntityList(data_source_oddrn=base_oddrn, items=flatten)
        return de_list
