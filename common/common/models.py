# common/models.py

from pydantic import BaseModel # type: ignore

class SharedModel(BaseModel):
    id: int
    name: str