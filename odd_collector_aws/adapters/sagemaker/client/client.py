from abc import ABC, abstractmethod
from typing import Optional, Iterable

from odd_collector_aws.adapters.sagemaker.domain.experiment import Experiment


class Client(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_experiments(self, experiments_name: Optional[str]) -> Iterable[Experiment]:
        raise NotImplementedError
