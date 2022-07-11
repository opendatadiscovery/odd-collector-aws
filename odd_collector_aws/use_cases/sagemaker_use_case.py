from typing import List

from more_itertools import flatten
from odd_models.models import DataEntity
from oddrn_generator import Generator

from odd_collector_aws.adapters.sagemaker.client.sagemaker_client import SagemakerClient
from odd_collector_aws.adapters.sagemaker.experiment_mapper import ExperimentMapper
from odd_collector_aws.use_cases.s3_use_case import S3UseCase


class SagemakerUseCase:
    def __init__(
        self,
        sagemaker_client: SagemakerClient,
        s3_use_case: S3UseCase,
        oddrn_generator: Generator,
    ):
        self.sagemaker_client = sagemaker_client
        self.s3_use_case = s3_use_case
        self.oddrn_generator = oddrn_generator

    def get_data_entities(self, experiments: List[str]) -> List[DataEntity]:
        experiments = list(self.sagemaker_client.get_experiments(experiments))

        mapper = ExperimentMapper(
            self.oddrn_generator, self.s3_use_case.oddrn_generator, self.s3_use_case
        )

        return list(flatten(map(mapper.map_experiment, experiments)))
