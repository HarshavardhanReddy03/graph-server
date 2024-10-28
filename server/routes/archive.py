from fastapi import APIRouter
from ..controllers import archive

router = APIRouter(tags=["archive"])


# Schema archives
@router.get("/archive/schema")
async def get_schema_archive_list():
    return await archive.get_schema_archive_list()


@router.get("/archive/schema/{timestamp}")
async def get_specific_schema_archive(timestamp: int):
    return await archive.get_specific_schema_archive(timestamp)


# State archives
@router.get("/archive/state")
async def get_state_archive_list():
    return await archive.get_state_archive_list()


@router.get("/archive/state/{timestamp}")
async def get_specific_state_archive(timestamp: int):
    return await archive.get_specific_state_archive(timestamp)
