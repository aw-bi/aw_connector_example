import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem

from aw_connector_example.dto import ObjectMeta


class ParquetService:
    """ """
    async def read_table(self, rows: list[dict]) -> pa.Table:
        return pa.Table.from_pylist(rows)
    
    async def write_table_s3(self, table: pa.Table, s3_path: str, s3fs: S3FileSystem):
        """ """
        pq.write_to_dataset(table, root_path=s3_path, filesystem=s3fs)

    async def write_table_fs(self, table: pa.Table, fs_path: str):
        """ """
        pq.write_to_dataset(table, root_path=fs_path)