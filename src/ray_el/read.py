"""Functions for reading data from DBAPI sources into Ray Datasets."""

from typing import Union, List
import ray
from ray.data import read_datasource, Dataset
from ray.util import ActorPool
from ray_el.datasources import DBAPIBasedDataSource


def read_sql(
    sql: Union[str, List[str]],
    DBAPIActorPool: ActorPool,
) -> Dataset:
    """Read data from SQL queries into a Ray Dataset.

    Creates a DBAPIBasedDataSource and reads it using ray.data.read_datasource(),
    returning a Ray Dataset that can be iterated over and processed block by block
    without waiting for all blocks to be produced.

    Args:
        sql: A SQL string or list of SQL strings representing SQL statements.
        DBAPIActorPool: A Ray ActorPool containing dbapi_actor instances.

    Returns:
        A Ray Dataset that streams blocks as they are produced.
    """
    datasource = DBAPIBasedDataSource(sql=sql, DBAPIActorPool=DBAPIActorPool)
    return read_datasource(datasource)
