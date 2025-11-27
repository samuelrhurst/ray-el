# ray-el

A data extract-load (EL) tool on top of the Ray distributed framework.

## Installation

```bash
pip install ray-el
```

## Usage

```python
import ray
from ray_el import read_sql, create_dbapi_actor_pool

# Initialize Ray
ray.init()

# Create a connection factory function
def get_connection():
    import sqlite3
    return sqlite3.connect(':memory:')

# Create an actor pool
actor_pool = create_dbapi_actor_pool(get_connection, n_actors=4)

# Read data using SQL
dataset = read_sql("SELECT * FROM table_name", actor_pool)

# Process blocks as they arrive
for block in dataset.iter_blocks():
    print(block)
```

## Documentation

For full documentation, visit [Zensical](https://zensical.org/docs/get-started/).
