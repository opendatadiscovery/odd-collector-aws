from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

from ..domain.experiment import Experiment


class Client(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_experiments(self, experiments_name: Optional[str]) -> Generator[Experiment, Any, Any]:
        raise NotImplementedError
