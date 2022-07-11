from abc import ABC

from pydantic import BaseModel


class BaseSagemakerEntity(BaseModel, ABC):
    class Config:
        @classmethod
        def alias_generator(cls, string: str) -> str:
            return "".join(word.capitalize() for word in string.split("_"))
