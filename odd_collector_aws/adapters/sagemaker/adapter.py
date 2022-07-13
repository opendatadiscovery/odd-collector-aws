from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from oddrn_generator.generators import S3Generator, SagemakerGenerator

from odd_collector_aws.adapters.s3.clients.s3_client import S3Client
from odd_collector_aws.adapters.s3.s3_dataset_service import S3DatasetService
from odd_collector_aws.adapters.sagemaker.client.sagemaker_client import SagemakerClient
from odd_collector_aws.utils.create_generator import create_generator
from odd_collector_aws.domain.plugin import SagemakerPlugin
from odd_collector_aws.use_cases.s3_use_case import S3UseCase
from odd_collector_aws.use_cases.sagemaker_use_case import SagemakerUseCase
from odd_collector_aws.use_cases.s3_dataset_use_case import S3DatasetUseCase


class Adapter(AbstractAdapter):
    def __init__(self, config: SagemakerPlugin):
        self.config = config

        s3_dataset_uc = S3DatasetUseCase(S3DatasetService(S3Client(config)))
        s3_generator = create_generator(S3Generator, config)
        s3_use_case = S3UseCase(s3_dataset_uc, s3_generator)

        self.client = SagemakerClient(self.config)
        self.oddrn_generator = create_generator(SagemakerGenerator, config)
        self.sagemaker_use_case = SagemakerUseCase(
            self.client, s3_use_case, self.oddrn_generator
        )

    def get_data_source_oddrn(self) -> str:
        return self.oddrn_generator.get_data_source_oddrn()

    # Error handling:
    def get_data_entity_list(self):
        data_entities = self.sagemaker_use_case.get_data_entities(
            self.config.experiments
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=data_entities,
        )
