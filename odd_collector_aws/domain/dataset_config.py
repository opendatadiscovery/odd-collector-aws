from typing import Optional

from pydantic import BaseModel, validator
from funcy import re_test


class DatasetConfig(BaseModel):
    bucket: str
    path: str
    partitioning: Optional[str] = None
    folder_as_dataset: bool = False

    @property
    def full_path(self) -> str:
        return f"{self.bucket}/{self.path.lstrip('/')}"

    @validator("path")
    def path_must_have_a_key(cls, path: str):
        if not re_test("\w", path):
            raise ValueError("must contain at least 1 key")
        return path
