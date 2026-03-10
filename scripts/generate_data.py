import numpy as np
import shapely
import zarr
from zarr.dtype import VariableLengthBytes, VariableLengthUTF8


# Root of the Zarr store
root = zarr.open_group("data/zarr_store.zarr", mode="w", zarr_format=3)

meta = root.create_group("meta")

date_data = np.array(["2023-01-01", "2023-01-02", "2023-01-03"], dtype="datetime64[ms]")
meta.create_array("date", data=date_data)

collection_data = ["collection_a", "collection_b", "collection_c"]
collection_array = meta.create_array(
    "collection",
    shape=(len(collection_data),),
    dtype=VariableLengthUTF8(),
)
collection_array[:] = collection_data


bbox_data = shapely.to_wkb(
    [
        shapely.box(-10.0, -10.0, 10.0, 10.0),
        shapely.box(-20.0, -20.0, 20.0, 20.0),
        shapely.box(-30.0, -30.0, 30.0, 30.0),
    ]
)

bbox_array = meta.create_array(
    "bbox",
    shape=(len(bbox_data),),
    dtype=VariableLengthBytes(),
)
bbox_array[:] = bbox_data
