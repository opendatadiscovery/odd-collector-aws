from datetime import datetime
from typing import Optional, Dict, List

import flatdict
from odd_models.models import DataEntity, DataEntityType, DataTransformer, MetadataExtension
from pydantic import validator

from .artifact import Artifact
from .base_object import BaseObject, ToDataEntity
from .source import Source


class UserInfo(BaseObject):
    user_profile_arn: Optional[str]
    user_profile_name: Optional[str]
    domain_id: Optional[str]


class TrialComponentStatus(BaseObject):
    primary_status: str
    message: str


class MetadataProperties(BaseObject):
    commit_id: str
    repository: str
    generated_by: str
    project_id: str


class Parameter(BaseObject):
    number_value: Optional[float]
    string_value: Optional[str]


class Metric(BaseObject):
    metric_name: str
    source_arn: str
    time_stamp: datetime
    max: float
    min: float
    last: float
    count: float
    avg: float
    std_dev: float


class TrialComponent(BaseObject, ToDataEntity):
    trial_component_name: str
    trial_component_arn: str
    display_name: str
    source: Source
    status: TrialComponentStatus
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    creation_time: datetime
    last_modified_time: datetime
    created_by: UserInfo
    last_modified_by: UserInfo
    parameters: Optional[Dict[str, Parameter]]
    input_artifacts: List[Artifact]
    output_artifacts: List[Artifact]
    metrics: List[Metric]

    @validator('trial_component_name')
    def passwords_match(cls, v: str):
        return '-'.join(v.split('-')[2:])

    def to_data_entity(
            self,
            oddrn: str,
            inputs: List[str] = [],
            outputs: List[str] = [],
    ) -> DataEntity:
        return DataEntity(
            oddrn=oddrn,
            name=self.trial_component_name,
            type=DataEntityType.JOB,
            metadata=self.__extract_metadata(),
            data_transformer=DataTransformer(
                inputs=inputs,
                outputs=outputs,
            ),
        )

    def __get_parameters_dict(self):
        res = {}
        for name, parameter in self.parameters.items():
            name = f'Parameter.{name}'
            if parameter.number_value is not None:
                res[name] = parameter.number_value
            else:
                res[name] = parameter.string_value
        return res

    def __get_metrics(self):
        res = dict()
        for m in self.metrics:
            for k, v in m.__dict__.items():
                uid = f'Metric.{m.metric_name}.{k}'
                res[uid] = v

        return res

    def __extract_metadata(self):
        schema = 'https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/TrialComponent'

        m = {
            'CreatedBy': flatdict.FlatDict(self.created_by),
            'CreationTime': self.creation_time,
            'Source': flatdict.FlatDict(self.source),
            **self.__get_parameters_dict()
        }

        if self.metrics:
            m.update(self.__get_metrics())

        return [MetadataExtension(schema_url=schema, metadata=flatdict.FlatDict(m))]
