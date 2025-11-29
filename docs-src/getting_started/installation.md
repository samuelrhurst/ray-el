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

| Option | Package | PyPI | Source |
|--------|---------|------|:------:|
| `bigquery` | adbc-driver-bigquery | [PyPI](https://pypi.org/project/adbc-driver-bigquery/) | [:fontawesome-brands-github:](https://github.com/apache/arrow-adbc) |
| `databricks` | databricks-sql-connector | [PyPI](https://pypi.org/project/databricks-sql-connector/) | [:fontawesome-brands-github:](https://github.com/databricks/databricks-sql-python) |
| `duckdb` | duckdb | [PyPI](https://pypi.org/project/duckdb/) | [:fontawesome-brands-github:](https://github.com/duckdb/duckdb) |
| `mssql` | mssql-python | [PyPI](https://pypi.org/project/mssql-python/) | [:fontawesome-brands-github:](https://github.com/pymssql/pymssql) |
| `mysql` | mysql-connector-python | [PyPI](https://pypi.org/project/mysql-connector-python/) | [:fontawesome-brands-github:](https://github.com/mysql/mysql-connector-python) |
| `odbc` | turbodbc | [PyPI](https://pypi.org/project/turbodbc/) | [:fontawesome-brands-github:](https://github.com/blue-yonder/turbodbc) |
| `oracle` | oracledb | [PyPI](https://pypi.org/project/oracledb/) | [:fontawesome-brands-github:](https://github.com/oracle/python-oracledb) |
| `postgresql` | adbc-driver-postgresql | [PyPI](https://pypi.org/project/adbc-driver-postgresql/) | [:fontawesome-brands-github:](https://github.com/apache/arrow-adbc) |
| `redshift` | redshift_connector | [PyPI](https://pypi.org/project/redshift-connector/) | [:fontawesome-brands-github:](https://github.com/aws/amazon-redshift-python-driver) |
| `snowflake` | adbc-driver-snowflake | [PyPI](https://pypi.org/project/adbc-driver-snowflake/) | [:fontawesome-brands-github:](https://github.com/apache/arrow-adbc) |
| `teradata` | teradatasql | [PyPI](https://pypi.org/project/teradatasql/) | [:fontawesome-brands-github:](https://github.com/Teradata/python-driver) |
| `trino` | trino | [PyPI](https://pypi.org/project/trino/) | [:fontawesome-brands-github:](https://github.com/trinodb/trino-python-client) |

### Installing Multiple Drivers

You can install multiple database drivers at once by separating them with commas:

```bash
pip install ray-el[postgresql,snowflake,duckdb]
```