import abc
from abc import ABC
from typing import Optional

import flatdict
from odd_models.models import DataEntity, DataEntityType, DataSet, DataConsumer, MetadataExtension, DataInput

from .base_object import ToDataEntity, BaseObject


def is_model(artifact_type: str) -> bool:
    return artifact_type == "ModelArtifact"


def is_image(artifact_type: str) -> bool:
    return artifact_type == "SageMaker.ImageUri"


class Artifact(BaseObject, ToDataEntity, ABC):
    artifact_type: str
    media_type: Optional[str]
    arn: Optional[str]
    name: str
    uri: str

    @abc.abstractmethod
    def to_data_entity(self, oddrn: str) -> DataEntity:
        raise NotImplementedError()

    def _extract_metadata(self):
        schema = 'https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/TrialComponent'

        m = {
            'Uri': self.uri,
            'ArtifactType': self.artifact_type,
            'MediaType': self.media_type,
            'Arn': self.arn,
            'Name': self.name,
        }

        return [MetadataExtension(schema_url=schema, metadata=flatdict.FlatDict(m))]


class Dataset(Artifact):
    def to_data_entity(self, oddrn: str, parent_oddrn: str = None) -> DataEntity:
        return DataEntity(
            oddrn=oddrn,
            name=self.name,
            type=DataEntityType.FILE,
            metadata=self._extract_metadata(),
            dataset=DataSet(parent_oddrn=parent_oddrn, field_list=[]),
        )


class Image(Artifact):
    def to_data_entity(self, oddrn: str, outputs=[]) -> DataEntity:
        return DataEntity(
            oddrn=oddrn,
            name=self.name,
            type=DataEntityType.MICROSERVICE,
            metadata=self._extract_metadata(),
            data_input=DataInput(outputs=outputs),
        )


class Model(Artifact):
    def to_data_entity(self, oddrn: str, inputs=[]) -> DataEntity:
        return DataEntity(
            oddrn=oddrn,
            name=self.name,
            type=DataEntityType.ML_MODEL,
            metadata=self._extract_metadata(),
            data_consumer=DataConsumer(inputs=inputs),
        )


def create_artifact(name: str, uri: str, media_type: str = None, arn: str = None) -> Artifact:
    if name == 'SageMaker.ImageUri':
        return Image(
            Name=uri.split('/')[-1],
            Uri=uri,
            ArtifactType='Image',
        )
    elif name == 'SageMaker.ModelArtifact':
        return Model(
            Name='model',
            Uri=uri,
            ArtifactType='Model',
            Arn=arn
        )
    else:
        return Dataset(
            ArtifactType='Dataset',
            MediaType=media_type,
            Arn=arn,
            Name=name,
            Uri=uri
        )
