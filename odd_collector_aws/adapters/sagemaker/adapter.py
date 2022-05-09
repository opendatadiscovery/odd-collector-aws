import itertools

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import SagemakerGenerator

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

        self.__oddrn_generator = SagemakerGenerator(
            cloud_settings={
                "region": config.aws_region,
                "account": self.client.account_id,
            }
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self):
        base_oddrn = self.get_data_source_oddrn()
        print(base_oddrn)
        experiments = self.client.get_experiments(self.config.experiments)

        data_entities = (map_experiment(i, self.__oddrn_generator) for i in experiments)

        return DataEntityList(data_source_oddrn=base_oddrn, items=list(itertools.chain(*data_entities)))
