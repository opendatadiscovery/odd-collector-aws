import traceback as tb
from typing import Iterable, Union

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import Generator, S3Generator

from odd_collector_aws.domain.plugin import S3Plugin
from odd_collector_aws.utils.create_generator import create_generator

from .file_system import FileSystem
from .logger import logger
from .mapper.bucket import map_bucket


class Adapter(AbstractAdapter):
    config: S3Plugin
    generator: Union[Generator, S3Generator]

    def __init__(self, config: S3Plugin) -> None:
        self.config = config
        self.generator = create_generator(S3Generator, config)
        self.fs = FileSystem(config)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> Iterable[DataEntityList]:
        for dataset_config in self.config.datasets:
            try:
                bucket = self.fs.get_bucket(dataset_config)
                data_entities = map_bucket(bucket, self.generator)

                yield DataEntityList(
                    data_source_oddrn=self.get_data_source_oddrn(),
                    items=list(data_entities),
                )
            except Exception as e:
                logger.error(
                    f"Error while processing bucket {dataset_config.bucket}: {e}."
                    " SKIPPING."
                )
                logger.debug(tb.format_exc())
                continue
