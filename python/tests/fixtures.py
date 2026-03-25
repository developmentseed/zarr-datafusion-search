from pathlib import Path

import icechunk
import numpy as np
import shapely
import zarr
from zarr.core.group import Group
from zarr.dtype import VariableLengthBytes, VariableLengthUTF8


def generate_test_data(root: Group) -> None:
    """Create test zarr data

    Adds to a zarr store:
    - a /meta group which contains
    - date: datetime64[ms] array with dates [2023-01-01, 2023-01-02,
      2023-01-03]
    - collection: variable length UTF8 array with ["collection_a",
      "collection_b", "collection_c"]
    - bbox: variable length bytes (WKB) array with boxes at (-10,-10,10,10),
      (-20,-20,20,20), (-30,-30,30,30)

    Args:
        root: The root Group in the target Zarr or Icechunk store.
    """
    meta = root.create_group("meta")

    # Create date array
    date_data = np.array(
        ["2023-01-01", "2023-01-02", "2023-01-03"], dtype="datetime64[ms]"
    )
    meta.create_array("date", data=date_data)

    # Create collection array
    collection_data = ["collection_a", "collection_b", "collection_c"]
    collection_array = meta.create_array(
        "collection",
        shape=(len(collection_data),),
        dtype=VariableLengthUTF8(),
    )
    collection_array[:] = collection_data

    # Create bbox array with WKB-encoded geometries
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


def create_test_zarr_data(store_path: Path | str) -> None:
    """Create test store data at the specified path.

    Args:
        store_path: Path where the Zarr store will be created. Can be string
            or Path object.

    """
    store_path = Path(store_path) if isinstance(store_path, str) else store_path
    root = zarr.open_group(store_path, mode="w", zarr_format=3)
    generate_test_data(root=root)


def create_test_icechunk_data(store_path: Path | str) -> None:
    """Create test icechunk store at the specified path.

    Args:
        store_path: Path where the Icechunk repository will be created.
            Can be string or Path object.

    """
    store_path = Path(store_path) if isinstance(store_path, str) else store_path

    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")

    root = zarr.open_group(session.store, mode="w", zarr_format=3)
    generate_test_data(root=root)
    session.commit("Initial test data")
