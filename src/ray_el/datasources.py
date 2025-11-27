"""Ray data sources for DBAPI-based data extraction."""

from typing import Any, Iterable, Union, List
import ray
from ray.data.datasource import Datasource
from ray.data.block import DelegatingBlockBuilder, Block
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

    def _read_stream(self, **kwargs: Any) -> Iterable[Block]:
        """Read data from the DBAPI actors as blocks.

        Submits each SQL statement to the actor pool and yields blocks as they
        are produced, converting chunks to blocks using DelegatingBlockBuilder.

        Yields:
            Ray blocks constructed from the streamed data chunks.
        """
        # Submit all SQL statements to the actor pool
        futures = []
        for sql_statement in self.sql:
            future = self.actor_pool.submit(
                lambda actor, sql: actor.stream_chunks.remote(sql),
                sql_statement,
            )
            futures.append(future)

        # Process results as they arrive
        for future in futures:
            result = ray.get(future)
            
            # result is a remote generator, so we need to get the actual chunks
            if hasattr(result, '__iter__'):
                builder = DelegatingBlockBuilder()
                for chunk in result:
                    for row in chunk:
                        builder.add(row)
                if builder.get_num_rows() > 0:
                    yield builder.build()
