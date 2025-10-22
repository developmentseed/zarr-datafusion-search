from pathlib import Path

import zarr_datafusion_search
from datafusion import SessionContext

ROOT_DIR = Path(__file__).parent.parent.parent


def test_zarr_scan():
    ctx = SessionContext()

    zarr_path = ROOT_DIR / "data" / "zarr_store.zarr"
    zarr_table = zarr_datafusion_search.ZarrTable(str(zarr_path), "/meta")

    ctx.register_table_provider("zarr_data", zarr_table)

    sql = "SELECT * FROM zarr_data;"
    df = ctx.sql(sql)
    df.show()
