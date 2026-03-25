import icechunk
import pytest
from datafusion import SessionContext
from geodatafusion import register_all
from obstore.store import LocalStore
from zarr_datafusion_search import ZarrTable


def test_zarr_scan(session_zarr_store):
    """Test basic zarr scanning with DataFusion."""
    ctx = SessionContext()
    zarr_table = ZarrTable(str(session_zarr_store), "/meta")

    ctx.register_table("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    df.show()


def test_spatial_functions_registered(session_zarr_store):
    """Test that spatial functions work with zarr data."""
    ctx = SessionContext()
    register_all(ctx)
    zarr_table = ZarrTable(str(session_zarr_store), "/meta")

    ctx.register_table("zarr_data", zarr_table)
    sql = (
        "SELECT collection FROM zarr_data "
        "WHERE ST_Intersects(bbox, "
        "ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'))"
    )
    df = ctx.sql(sql)
    df.show()


@pytest.mark.asyncio
async def test_zarr_scan_from_obstore(session_zarr_store):
    """Test zarr scanning from object store."""
    store = LocalStore(session_zarr_store)
    zarr_table = await ZarrTable.from_obstore(store, "/meta")

    ctx = SessionContext()

    ctx.register_table("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    print(df.schema())
    df.show()


@pytest.mark.asyncio
async def test_zarr_scan_from_icechunk(session_icechunk_store):
    """Test zarr scanning from icechunk."""
    storage = icechunk.local_filesystem_storage(session_icechunk_store)
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")

    zarr_table = await ZarrTable.from_icechunk(session=session, group_path="/meta")

    ctx = SessionContext()

    ctx.register_table("icechunk_data", zarr_table)

    sql = "SELECT * FROM icechunk_data;"
    df = ctx.sql(sql)
    print(df.schema())
    df.show()
