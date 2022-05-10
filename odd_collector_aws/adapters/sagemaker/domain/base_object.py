from abc import ABC, abstractmethod

from odd_models.models import DataEntity
from pydantic import BaseModel


class ToDataEntity(ABC):
    @abstractmethod
    def to_data_entity(self, *args, **kwargs) -> DataEntity:
        raise NotImplementedError()


class BaseObject(BaseModel, ABC):
    class Config:
        @classmethod
        def alias_generator(cls, string: str) -> str:
            return "".join(word.capitalize() for word in string.split("_"))
