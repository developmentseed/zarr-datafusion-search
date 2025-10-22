from obstore.store import ObjectStore

class ZarrTable:
    def __init__(self, base_path: str, group_path: str) -> None: ...
    def __datafusion_table_provider__(self) -> object: ...
    @classmethod
    async def from_obstore(cls, store: ObjectStore, group_path: str) -> ZarrTable:
        """Open a Zarr table."""
