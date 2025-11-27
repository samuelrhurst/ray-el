"""Ray actors for DBAPI-based data extraction."""

from typing import Callable, Any, Tuple
import ray
from ray.util import ActorPool


@ray.remote
class dbapi_actor:
    """Ray remote actor for executing SQL queries and streaming results via DBAPI."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        """Initialize the DBAPI actor.

        Args:
            connection_factory: A callable that takes no arguments and returns a DBAPI connection.
        """
        self.conn = connection_factory()

    def get_next_chunk(
        self, sql: str, chunk_size: int = 100000, cursor_id: str = None
    ) -> Tuple[list[tuple], str, bool]:
        """Get the next chunk of data from a SQL query.

        Args:
            sql: SQL statement to execute.
            chunk_size: Number of rows to fetch per chunk. Defaults to 100000.
            cursor_id: Identifier for an existing cursor, or None to create new one.

        Returns:
            A tuple of (rows, cursor_id, is_complete) where:
            - rows: List of row tuples fetched from the query
            - cursor_id: Identifier for the cursor (for subsequent calls)
            - is_complete: True if no more data available, False otherwise
        """
        # Create or retrieve cursor
        if cursor_id is None:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor_id = id(cursor)
            if not hasattr(self, '_cursors'):
                self._cursors = {}
            self._cursors[cursor_id] = cursor
        else:
            cursor = self._cursors.get(cursor_id)
            if cursor is None:
                return [], cursor_id, True
        
        # Fetch next chunk
        rows = cursor.fetchmany(chunk_size)
        
        # Check if complete
        is_complete = len(rows) == 0
        
        # Clean up if complete
        if is_complete:
            cursor.close()
            if hasattr(self, '_cursors') and cursor_id in self._cursors:
                del self._cursors[cursor_id]
        
        return rows, cursor_id, is_complete


def create_dbapi_actor_pool(
    connection_factory: Callable[[], Any], n_actors: int = 1
) -> ActorPool:
    """Create a pool of DBAPI actors.

    Args:
        connection_factory: A callable that takes no arguments and returns a DBAPI connection.
        n_actors: Number of actors to create in the pool (default: 1).

    Returns:
        A Ray ActorPool containing the created dbapi_actor instances.
    """
    actors = [dbapi_actor.remote(connection_factory) for _ in range(n_actors)]
    return ActorPool(actors)
