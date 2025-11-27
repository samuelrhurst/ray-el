"""Ray data sources for DBAPI-based data extraction."""

from typing import Union, List, Optional
import ray
from ray.data.datasource import Datasource, ReadTask
from ray.data.block import Block
from ray.data.read_api import DelegatingBlockBuilder
from ray.util import ActorPool


class DBAPIBasedDataSource(Datasource):
    """A Ray data source that reads from a DBAPI connection using SQL queries."""

    def __init__(
        self,
        sql: Union[str, List[str]],
        DBAPIActorPool: ActorPool,
    ) -> None:
        """Initialize the DBAPIBasedDataSource.

        Args:
            sql: A SQL string or list of SQL strings representing SQL statements.
            DBAPIActorPool: A Ray ActorPool containing dbapi_actor instances.
        """
        self.sql = sql if isinstance(sql, list) else [sql]
        self.actor_pool = DBAPIActorPool

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate the in-memory data size.
        
        Returns:
            None since we don't know the size in advance.
        """
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Get read tasks for the datasource.

        Args:
            parallelism: The number of parallel read tasks.

        Returns:
            A list of ReadTask objects that execute the SQL queries.
        """
        def read_fn(sql_statement: str) -> Block:
            """Execute a SQL statement and return a block of results."""
            # Submit the SQL statement to an actor in the pool
            future = self.actor_pool.submit(
                lambda actor, sql: actor.stream_chunks.remote(sql),
                sql_statement,
            )
            result = ray.get(future)
            
            # Build a block from the streamed chunks
            builder = DelegatingBlockBuilder()
            if hasattr(result, '__iter__'):
                for chunk in result:
                    for row in chunk:
                        builder.add(row)
            
            return builder.build()

        # Create a read task for each SQL statement
        read_tasks = []
        for sql_statement in self.sql:
            read_task = ReadTask(
                lambda sql=sql_statement: read_fn(sql),
                metadata=None,
            )
            read_tasks.append(read_task)

        return read_tasks
