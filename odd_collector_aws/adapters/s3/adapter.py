from importlib.metadata import metadata
import logging
from dataclasses import dataclass
from typing import List, Dict, Union, Iterable, Any

from odd_models.models import DataEntity
from oddrn_generator.generators import S3Generator

from odd_s3_adapter.mapper.dataset import map_dataset
from odd_s3_adapter.schema.s3_parquet_schema_retriever import S3SchemaRetriever
import time

class S3Adapter:
    def __init__(self,
                 oddrn_generator: S3Generator,
                 s3_paths: List[str],
                 schema_retriever: S3SchemaRetriever) -> None:
        self.__schema_retriever = schema_retriever
        self.__oddrn_generator = oddrn_generator
        self.__s3_paths = s3_paths

    def get_entities(self) -> Iterable[DataEntity]: 
        for path in self.__s3_paths:
            start = time.time()
            logging.info(f"Starting metadat fetch for {path}")
            self.__oddrn_generator.set_oddrn_paths(buckets=path.split('/')[0])
            file_format = self.__schema_retriever.get_format(path)
            if file_format:
                schema = self.__schema_retriever.get_schema(path, file_format)
                if schema:
                    metadata = self.__schema_retriever.get_metadata(path, file_format)
                    logging.info(f"finishing sucsessfull fetch for {path} during {time.time()-start} seconds")
                    yield map_dataset(
                        name=path,
                        schema=schema,
                        metadata=metadata,
                        oddrn_gen=self.__oddrn_generator
                    )
