from fastapi import APIRouter
from ..controllers import state
from ..models.change import Change

router = APIRouter(tags=["state"])


@router.get("/state/live")
async def get_live_state():
    return await state.get_live_state()


@router.post("/state/live/update")
async def update_live_state(update: Change):
    return await state.queue_live_state_update(update)
