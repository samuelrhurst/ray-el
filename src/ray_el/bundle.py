from typing import Any, Sequence
import ray


class bundle:
    """A bundle of pipelines that can be run on a Ray cluster."""

    def __init__(self, futures: Sequence[ray.ObjectRef]) -> None:
        """Initialize the bundle with a sequence of pipeline ObjectRefs.
        
        Args:
            futures: A sequence of Ray ObjectRefs representing pipeline executions.
        """
        self.futures = futures
        self.results: Any = None

    def run(self) -> None:
        """Execute ray.get() on the futures and store results."""
        self.results = ray.get(self.futures)
