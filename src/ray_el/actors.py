"""Ray actors for DBAPI-based data extraction."""

from typing import Callable, Iterator, Any, Tuple
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

    def stream_chunks(
        self, sql: str, chunk_size: int = 100000
    ) -> Iterator[list[dict]]:
        """Stream chunks of data from a SQL query.

        Args:
            sql: SQL statement to execute.
            chunk_size: Number of rows to fetch per chunk. Defaults to 100000.

        Yields:
            Lists of rows (as dictionaries or tuples) fetched from the query.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                yield rows


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
