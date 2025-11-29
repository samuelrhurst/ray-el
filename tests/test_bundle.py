import pytest
import ray
from ray_el.bundle import bundle
from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def ray_context():
    """Initialize Ray for the test module."""
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


def test_bundle_run():
    """Test that bundle correctly resolves ObjectRefs from ray.data.read_parquet()."""
    # Get path to penguins.parquet in tests directory
    test_dir = Path(__file__).parent
    parquet_path = test_dir / "penguins.parquet"
    
    # Create a remote function that returns the arrow refs
    @ray.remote
    def load_dataset_refs(path):
        dataset = ray.data.read_parquet(path)
        return dataset.to_arrow_refs()
    
    # Create two ObjectRefs from reading the parquet file
    ref1 = load_dataset_refs.remote(str(parquet_path))
    ref2 = load_dataset_refs.remote(str(parquet_path))
    
    # Create a sequence of the ObjectRefs
    futures = [ref1, ref2]
    
    # Create bundle instance
    test_bundle = bundle(futures)
    
    # Verify futures attribute is set
    assert test_bundle.futures == futures
    
    # Verify results is None before run()
    assert test_bundle.results is None
    
    # Run the bundle
    test_bundle.run()
    
    # Verify results is set after run()
    assert test_bundle.results is not None
    
    # Verify we got results for both refs
    assert len(test_bundle.results) == 2
