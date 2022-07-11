from abc import ABC, abstractmethod

from odd_models.models import DataEntity


class ToDataEntity(ABC):
    """
    Interface for models which can be mapped to DataEntity
    """

    @abstractmethod
    def to_data_entity(self, *args, **kwargs) -> DataEntity:
        raise NotImplementedError
