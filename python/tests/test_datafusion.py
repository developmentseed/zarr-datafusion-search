import zarr_datafusion_internal
from datafusion import SessionContext


def test_zarr_scan():
    ctx = SessionContext()

    zarr_path = "../../data/zarr_store.zarr"
    zarr_table = zarr_datafusion_internal.ZarrTable(zarr_path)

    ctx.register_table_provider("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    df.show()
