from odd_collector_aws.adapters.s3.clients.s3_client import S3Client
from odd_collector_aws.adapters.s3.s3_dataset_service import S3DatasetService
from odd_collector_aws.domain.plugin import S3Plugin
from odd_collector_aws.use_cases.s3_dataset_use_case import S3DatasetUseCase
from odd_collector_aws.use_cases.s3_use_case import S3UseCase
from odd_collector_aws.utils.create_generator import create_generator
from odd_collector_aws.utils.handle_nested_structure import HandleNestedStructure
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import S3Generator
from funcy import lpluck_attr, lconcat

from .logger import logger


class Adapter(AbstractAdapter):
    def __init__(self, config: S3Plugin) -> None:
        try:
            self.__datasets = config.datasets

            self._oddrn_generator = create_generator(S3Generator, config)
            self.s3_client = S3Client(config)
            dataset_client = S3DatasetService(self.s3_client)
            dataset_use_case = S3DatasetUseCase(dataset_client)

            self.s3_use_case = S3UseCase(dataset_use_case, self._oddrn_generator)
            self.handle_obj = HandleNestedStructure()
        except Exception:
            logger.debug("Error during initialization adapter", exc_info=True)

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = lconcat(self._get_entities())
        list_of_oddrns = lpluck_attr("oddrn", entities)
        folder_data_entities = self.handle_obj.get_all_data_entities(
            list(reversed(list_of_oddrns)),
            self.__datasets,
            self._oddrn_generator,
            self.s3_client.s3_folders
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=entities + folder_data_entities,
        )

    def _get_entities(self):
        """
        Read all files from s3 and generate DataEntity objects
        """
        if not self.__datasets:
            logger.warning("No datasets configured")
            return []

        for dataset in self.__datasets:
            self._oddrn_generator.set_oddrn_paths(buckets=dataset.bucket)

            try:
                logger.debug(f"Getting info for: {dataset}")
                yield from self.s3_use_case.get_data_entities(dataset)
                logger.debug(f"Getting info for: {dataset}")
            except Exception:
                logger.error(
                    f"Got unexpected error for {dataset.path}, SKIP object.",
                    exc_info=True,
                )
                continue
