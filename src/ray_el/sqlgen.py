"""SQL generation utilities using SQLGlot for data loading operations."""

from typing import Union, List, Dict, Any
import sqlglot
from sqlglot.expressions import Schema, ColumnDef, DataType


def generate_create_table_stmts(
    table: Union[str, List[str]],
    schema: Union[Dict[str, str], List[Dict[str, str]]],
    dialect: str,
) -> List[str]:
    """Generate CREATE TABLE statements.

    Args:
        table: A table name string or list of table name strings.
        schema: A dictionary of column names to datatypes, or a list of such dictionaries.
        dialect: SQLGlot dialect to use for SQL generation.

    Returns:
        A list of CREATE TABLE SQL statements.
    """
    tables = table if isinstance(table, list) else [table]
    schemas = schema if isinstance(schema, list) else [schema]

    # If single schema provided for multiple tables, use it for all
    if len(schemas) == 1 and len(tables) > 1:
        schemas = schemas * len(tables)

    statements = []
    for tbl, tbl_schema in zip(tables, schemas):
        # Build column definitions
        columns = []
        for col_name, col_type in tbl_schema.items():
            col_def = ColumnDef(
                this=sqlglot.exp.Identifier(this=col_name),
                kind=sqlglot.parse_one(col_type, into=DataType),
            )
            columns.append(col_def)

        # Create table expression
        create_table = sqlglot.exp.Create(
            this=sqlglot.exp.Schema(
                this=sqlglot.exp.Table(name=tbl),
                expressions=columns,
            )
        )

        statements.append(create_table.sql(dialect=dialect))

    return statements


def generate_insert_into_from_table_stmt(
    staging_table: Union[str, List[str]],
    target_table: str,
    dialect: str,
) -> str:
    """Generate an INSERT INTO statement from staging table(s).

    If multiple staging tables are provided, they will be unioned in the INSERT statement.

    Args:
        staging_table: A staging table name string or list of staging table name strings.
        target_table: The target table name string.
        dialect: SQLGlot dialect to use for SQL generation.

    Returns:
        An INSERT INTO SQL statement.
    """
    staging_tables = (
        staging_table if isinstance(staging_table, list) else [staging_table]
    )

    # Create SELECT statements from each staging table
    select_stmts = [
        sqlglot.exp.Select(expressions=[sqlglot.exp.Star()]).from_(tbl)
        for tbl in staging_tables
    ]

    # Union all staging table selects if multiple
    if len(select_stmts) > 1:
        select_union = select_stmts[0]
        for select_stmt in select_stmts[1:]:
            select_union = sqlglot.exp.Union(this=select_union, expression=select_stmt)
        select_from = select_union
    else:
        select_from = select_stmts[0]

    # Create INSERT INTO statement
    insert_stmt = sqlglot.exp.Insert(
        this=sqlglot.exp.Table(name=target_table),
        expression=select_from,
    )

    return insert_stmt.sql(dialect=dialect)


def generate_drop_table_stmts(
    table: Union[str, List[str]],
    dialect: str,
) -> List[str]:
    """Generate DROP TABLE statements.

    Args:
        table: A table name string or list of table name strings.
        dialect: SQLGlot dialect to use for SQL generation.

    Returns:
        A list of DROP TABLE SQL statements.
    """
    tables = table if isinstance(table, list) else [table]

    statements = []
    for tbl in tables:
        drop_table = sqlglot.exp.Drop(this=sqlglot.exp.Table(name=tbl))
        statements.append(drop_table.sql(dialect=dialect))

    return statements
