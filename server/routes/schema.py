from fastapi import APIRouter
from ..controllers import schema
from ..models.change import Change

router = APIRouter(tags=["schema"])


@router.get("/schema/live")
async def get_live_schema():
    return await schema.get_live_schema()


@router.post("/schema/live/update")
async def update_live_schema(update: Change):
    return await schema.queue_live_schema_update(update)