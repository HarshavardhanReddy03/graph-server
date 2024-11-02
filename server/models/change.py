from pydantic import BaseModel
from typing import Dict, Literal, Optional


class Change(BaseModel):
    timestamp: int
    type: Literal["schema", "state"]
    action: str
    data: Dict
    version: Optional[str] = None
