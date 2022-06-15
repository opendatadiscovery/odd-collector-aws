import logging
from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector_aws.domain.plugin import S3Plugin, DatasetConfig
from .client import Client
from .dataset import S3Dataset
from .file_system import FileSystem
from .mapper.dataset import map_dataset
from .strategy import folder_strategy, file_strategy, each_file_strategy


class Adapter(AbstractAdapter):
    def __init__(self, config: S3Plugin) -> None:
        self.__datasets = config.datasets

        self.s3_client = Client(config)
        self.fs = FileSystem(config)

        self.__oddrn_generator = self.s3_client.get_oddrn_generator()

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(self._get_entities()),
        )

    def _get_entities(self):
        """
        Read all files from s3 and generate DataEntity objects
        """
        if not self.__datasets:
            logging.debug("No datasets configured")
            return []

        for dataset in self.__datasets:
            self.__oddrn_generator.set_oddrn_paths(buckets=dataset.bucket)

            try:
                for s3_dataset in self.get_s3_datasets_for(dataset):
                    yield map_dataset(
                        s3_dataset=s3_dataset, oddrn_gen=self.__oddrn_generator
                    )
            except Exception as e:
                logging.error(
                    f"Got unexpected error for {dataset.path}, SKIP.", exc_info=True
                )
                logging.error(e)
                continue

    def get_s3_datasets_for(self, dataset_config: DatasetConfig) -> List[S3Dataset]:
        """
        Choose the strategy to get the datasets
        """
        if dataset_config.each_file_as_dataset:
            datasets = each_file_strategy(dataset_config, self.s3_client, self.fs)
        elif dataset_config.path.endswith("/"):
            datasets = folder_strategy(dataset_config, self.s3_client, self.fs)
        else:
            datasets = file_strategy(dataset_config, self.fs)

        return datasets
