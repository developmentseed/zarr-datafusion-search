from pathlib import Path

import icechunk
import pytest
from datafusion import SessionContext
from obstore.store import LocalStore
from zarr_datafusion_search import ZarrTable

ROOT_DIR = Path(__file__).parent.parent.parent


def test_zarr_scan():
    ctx = SessionContext()

    zarr_path = ROOT_DIR / "data" / "zarr_store.zarr"
    zarr_table = ZarrTable(str(zarr_path), "/meta")

    ctx.register_table_provider("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    df.show()


@pytest.mark.asyncio
async def test_zarr_scan_from_obstore():
    store = LocalStore(ROOT_DIR / "data" / "zarr_store.zarr")
    zarr_table = await ZarrTable.from_obstore(store, "/meta")

    ctx = SessionContext()

    ctx.register_table_provider("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    df.show()


@pytest.mark.asyncio
async def test_zarr_scan_from_icechunk():
    storage = icechunk.local_filesystem_storage(ROOT_DIR / "data" / "icechunk")
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main") 

    zarr_table = await ZarrTable.from_icechunk(session=session, group_path="/meta")

    ctx = SessionContext()

    ctx.register_table_provider("icechunk_data", zarr_table)

    sql = "SELECT * FROM icechunk_data;"
    df = ctx.sql(sql)
    df.show()
