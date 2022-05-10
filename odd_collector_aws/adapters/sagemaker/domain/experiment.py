from datetime import datetime
from typing import List

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataEntityGroup,
    MetadataExtension,
)

from .base_object import BaseObject, ToDataEntity
from .source import Source
from .trial import Trial


class Experiment(BaseObject, ToDataEntity):
    experiment_arn: str
    experiment_name: str
    source: Source
    creation_time: datetime
    last_modified_time: datetime
    trials: List[Trial] = []

    def to_data_entity(self, oddrn: str) -> DataEntity:
        return DataEntity(
            oddrn=oddrn,
            name=self.experiment_name,
            metadata=self.__extract_metadata(),
            type=DataEntityType.ML_EXPERIMENT,
            data_entity_group=DataEntityGroup(entities_list=[], group_oddrn=oddrn),
        )

    def __extract_metadata(self):
        schema = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/Trial"

        metadata = {
            "ExperimentArn": self.experiment_arn,
            "CreationTime": self.creation_time,
            "LastModifiedTime": self.last_modified_time,
        }

        return [MetadataExtension(schema_url=schema, metadata=metadata)]
