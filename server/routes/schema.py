from fastapi import APIRouter, Depends
from ..controllers import schema
from ..models.change import Change

router = APIRouter(tags=["schema"])


@router.get("/schema/live/{version}")
async def get_live_schema(version: str):
    return await schema.get_live_schema(version)


@router.post("/schema/live/{version}/update")
async def update_live_schema(update: Change, version: str):
    return await schema.update_live_schema(update, version)
