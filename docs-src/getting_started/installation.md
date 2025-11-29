# Installation

## Basic Installation

Install ray-el using pip:

```bash
pip install ray-el
```

## Optional Database Drivers

ray-el supports various database systems through optional dependencies. You can install support for specific databases by specifying the appropriate extra during installation:

```bash
pip install ray-el[database_name]
```

### Available Database Drivers

| Option | Package | Documentation |
|--------|---------|---------------|
| `bigquery` | adbc-driver-bigquery | [PyPI](https://pypi.org/project/adbc-driver-bigquery/) |
| `databricks` | databricks-sql-connector | [PyPI](https://pypi.org/project/databricks-sql-connector/) |
| `duckdb` | duckdb | [PyPI](https://pypi.org/project/duckdb/) |
| `mssql` | mssql-python | [PyPI](https://pypi.org/project/mssql-python/) |
| `mysql` | mysql-connector-python | [PyPI](https://pypi.org/project/mysql-connector-python/) |
| `odbc` | turbodbc | [PyPI](https://pypi.org/project/turbodbc/) |
| `oracle` | oracledb | [PyPI](https://pypi.org/project/oracledb/) |
| `postgresql` | adbc-driver-postgresql | [PyPI](https://pypi.org/project/adbc-driver-postgresql/) |
| `redshift` | redshift_connector | [PyPI](https://pypi.org/project/redshift-connector/) |
| `snowflake` | adbc-driver-snowflake | [PyPI](https://pypi.org/project/adbc-driver-snowflake/) |
| `teradata` | teradatasql | [PyPI](https://pypi.org/project/teradatasql/) |
| `trino` | trino-python-client | [PyPI](https://pypi.org/project/trino-python-client/) |

### Installing Multiple Drivers

You can install multiple database drivers at once by separating them with commas:

```bash
pip install ray-el[postgresql,snowflake,duckdb]
```