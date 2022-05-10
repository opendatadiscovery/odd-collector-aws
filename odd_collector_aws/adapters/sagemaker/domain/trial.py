from datetime import datetime
from typing import List, Type

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataEntityGroup,
    MetadataExtension,
)
from oddrn_generator import Generator

from .base_object import BaseObject, ToDataEntity
from .source import Source
from .trial_component import TrialComponent


class Trial(BaseObject, ToDataEntity):
    trial_arn: str
    trial_name: str
    trial_source: Source
    creation_time: datetime
    last_modified_time: datetime
    trial_components: List[TrialComponent] = []

    def to_data_entity(self, oddrn_gen: Type[Generator]) -> DataEntity:
        return DataEntity(
            oddrn=oddrn_gen.get_oddrn_by_path("trials"),
            name=self.trial_name,
            metadata=self.__extract_metadata(),
            type=DataEntityType.ML_EXPERIMENT,
            data_entity_group=DataEntityGroup(
                entities_list=[],
                group_oddrn=oddrn_gen.get_oddrn_by_path("experiments"),
            ),
        )

    def __extract_metadata(self):
        schema = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/Trial"

        metadata = {
            "TrialArn": self.trial_arn,
            "CreationTime": self.creation_time,
            "LastModifiedTime": self.last_modified_time,
        }

        return [MetadataExtension(schema_url=schema, metadata=metadata)]
