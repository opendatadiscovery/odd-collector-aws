import logging
import time
import boto3

from typing import Iterable

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector_aws.domain.plugin import S3Plugin
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import S3Generator

from .mapper.dataset import map_dataset
from .schema.s3_schema_retriever import S3SchemaRetriever


class Adapter(AbstractAdapter):
    def __init__(self, config: S3Plugin) -> None:
        self.__s3_paths = config.paths
        self.__schema_retriever = S3SchemaRetriever(
            config.aws_access_key_id, config.aws_secret_access_key, config.aws_region
        )

        account_id = boto3.client(
            "sts",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        ).get_caller_identity()["Account"]

        self.__oddrn_generator = S3Generator(
            cloud_settings={"region": config.aws_region, "account": account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(self.get_entities()),
        )

    def get_entities(self) -> Iterable[DataEntity]:
        for path in self.__s3_paths:
            start = time.time()
            logging.info(f"Starting metadat fetch for {path}")
            self.__oddrn_generator.set_oddrn_paths(buckets=path.split("/")[0])
            if s3ds := self.__schema_retriever.build_s3ds(path):
                if schema := s3ds.get_schema():
                    metadata = s3ds.get_metadata()
                    logging.info(
                        f"finishing sucsessfull fetch for {path} during"
                        f" {time.time()-start} seconds"
                    )
                    yield map_dataset(
                        name=path,
                        schema=schema,
                        metadata=metadata,
                        oddrn_gen=self.__oddrn_generator,
                        rows_number=s3ds.get_rows(),
                    )
