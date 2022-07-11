from functools import singledispatchmethod
from typing import Union

from oddrn_generator.generators import S3Generator

from odd_collector_aws.adapters.s3.mapper.dataset import map_dataset
from odd_collector_aws.domain.plugin import DatasetConfig
from odd_collector_aws.use_cases.s3_dataset_use_case import S3DatasetUseCase


class S3UseCase:
    """
    Entry point to get DataEntities for s3 objects.


    parameters:
        s3_dataset_client - for retrieving S3Datasets
        oddrn_generator - for mapping S3Dataset -> DataEntity
    """

    def __init__(
        self, s3_dataset_use_case: S3DatasetUseCase, oddrn_generator: S3Generator
    ):
        self.s3_dataset_use_case = s3_dataset_use_case
        self.oddrn_generator = oddrn_generator

    def get_data_entities(self, arg: Union[str, DatasetConfig]):
        return [
            map_dataset(ds, self.oddrn_generator)
            for ds in self.s3_dataset_use_case.get_datasets(arg)
        ]
