import icechunk
import zarr
import numpy as np
import shapely
from zarr.dtype import VariableLengthUTF8

location = "data/icechunk"
storage = icechunk.local_filesystem_storage(location)
repo = icechunk.Repository.create(storage)
session = repo.writable_session("main")

root = zarr.open_group(session.store, mode="w", zarr_format=3)
meta = root.create_group("meta")

date_data = np.array(["2023-01-01", "2023-01-02", "2023-01-03"], dtype="datetime64[ms]")
meta.create_array("date", data=date_data)

meta.create_array(
    "collection",
    shape=(3,),
    dtype=VariableLengthUTF8(),
)
meta["collection"][...] = ["collection_a", "collection_b", "collection_c"]

bbox_data = shapely.to_wkt(
    [
        shapely.box(-10.0, -10.0, 10.0, 10.0),
        shapely.box(-20.0, -20.0, 20.0, 20.0),
        shapely.box(-30.0, -30.0, 30.0, 30.0),
    ]
)

meta.create_array(
    "bbox",
    shape=len(bbox_data),
    dtype=VariableLengthUTF8(),
)
meta["bbox"][...] = bbox_data

session.commit("First")
