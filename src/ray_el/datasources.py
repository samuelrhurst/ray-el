"""Ray data sources for DBAPI-based data extraction."""

from typing import Union, List, Optional, Iterator
import ray
from ray.data.datasource import Datasource, ReadTask
from ray.data.block import Block, BlockMetadata
from ray.util import ActorPool


class DBAPIBasedDataSource(Datasource):
    """A Ray data source that reads from a DBAPI connection using SQL queries."""

    def __init__(
        self,
        sql: Union[str, List[str]],
        DBAPIActorPool: ActorPool,
        chunk_size: int = 100000,
    ) -> None:
        """Initialize the DBAPIBasedDataSource.

        Args:
            sql: A SQL string or list of SQL strings representing SQL statements.
            DBAPIActorPool: A Ray ActorPool containing dbapi_actor instances.
            chunk_size: Number of rows to fetch per chunk. Defaults to 100000.
        """
        self.sql = sql if isinstance(sql, list) else [sql]
        self.actor_pool = DBAPIActorPool
        self.chunk_size = chunk_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate the in-memory data size.
        
        Returns:
            None since we don't know the size in advance.
        """
        return None

    def get_read_tasks(self, parallelism: int, per_task_row_limit: Optional[int] = None) -> List[ReadTask]:
        """Get read tasks for the datasource.

        Args:
            parallelism: The number of parallel read tasks.
            per_task_row_limit: Optional limit on rows per task.

        Returns:
            A list of ReadTask objects that execute the SQL queries.
        """
        def read_fn(sql_statement: str) -> Iterator[Block]:
            """Execute a SQL statement and stream blocks as chunks arrive."""
            import pyarrow as pa
            
            # Get an idle actor from the pool  
            if self.actor_pool.has_free():
                actor_handle = self.actor_pool.pop_idle()
            else:
                # If no actors are free, just get the first one
                actor_handle = list(self.actor_pool._idle_actors | self.actor_pool._busy_actors)[0]
            
            cursor_id = None
            while True:
                # Call the actor method with .remote() and wait for result
                rows, cursor_id, is_complete = ray.get(
                    actor_handle.get_next_chunk.remote(sql_statement, self.chunk_size, cursor_id)
                )
                if is_complete or not rows:
                    break
                
                if rows:
                    # Create Arrow table from rows (Arrow-based block as required)
                    # Convert rows to dictionaries with string column names
                    table = pa.Table.from_pylist(
                        [{f"col_{i}": val for i, val in enumerate(row)} for row in rows]
                    )
                    yield table
            
            # Return the actor to the pool
            self.actor_pool.push(actor_handle)

        # Create a read task for each SQL statement
        read_tasks = []
        for sql_statement in self.sql:
            # Create metadata with unknown size since we can't know in advance
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )
            read_task = ReadTask(
                lambda sql=sql_statement: read_fn(sql),
                metadata=metadata,
            )
            read_tasks.append(read_task)

        return read_tasks
