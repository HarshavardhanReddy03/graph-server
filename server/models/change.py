from pydantic import BaseModel
from typing import Dict, Literal


class Change(BaseModel):
    timestamp: int
    type: Literal["schema", "state"]
    action: str
    data: Dict
