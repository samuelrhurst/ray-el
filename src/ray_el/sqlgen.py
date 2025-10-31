import sqlglot
from typing import List, Union
from datetime import date
from datetime import datetime
from itertools import chain

def is_iso_date(s: str):
    try:
        date.fromisoformat(s)
        return True
    except ValueError:
        return False
    
def is_iso_datetime(s: str):
    try:
        datetime.fromisoformat(s)
        try:
            date.fromisoformat(s)
            return False
        except ValueError:
            return True
        return True
    except ValueError:
        return False

def generate_create_table_stmt():
    stmt = None
    return stmt

def generate_insert_into_from_table_stmt(staging_table: Union[str, List[str]], target_table: str, select_cols: List[Union[str]] = None, dialect: str=None):
    if isinstance(staging_table, str):
        # A single staging table
        if select_cols is not None:
            select_exprs = [sqlglot.expressions.Column(this=col) for col in select_cols]
            select_query = sqlglot.select(*select_exprs).from_(staging_table)
        else:
            select_query = sqlglot.select("*").from_(staging_table)
    else:
        # Multiple staging tables needing to be Unioned
        if select_cols is not None:
            select_exprs = [sqlglot.expressions.Column(this=col) for col in select_cols]
            selects = [sqlglot.select("select_exprs").from_(table) for table in staging_table]
        else:
            selects = [sqlglot.select("*").from_(table) for table in staging_table]
        select_query = selects[0]
        for select in selects[1:]:
            select_query = sqlglot.expressions.Union(this=select_query, expression=select, distinct=False)

    # Generating the insert statement
    insert_query = sqlglot.expressions.insert(into=target_table, expression=select_query)

    return insert_query.sql(dialect=dialect)

def generate_drop_table_stmts(table: Union[str, List[str]], dialect: str=None):
    stmts = []

    if isinstance(table, str):
        drop_stmt = sqlglot.expressions.Drop(
            this=sqlglot.expressions.Table(this=sqlglot.expressions.Identifier(this=table)),
            kind="TABLE"
        )
        stmts.append(drop_stmt.sql(dialect=dialect))
    else:
        for tbl in table:
            drop_stmt = sqlglot.expressions.Drop(
                this=sqlglot.expressions.Table(this=sqlglot.expressions.Identifier(this=tbl)),
                kind="TABLE"
            )
            stmts.append(drop_stmt.sql(dialect=dialect))

    return stmts

def generate_partitioned_queries(
    full_table_name: str,
    select_cols: List[Union[str, int, float]] = None,
    global_filter: str=None,
    partition_column: str=None,
    partition_values: List[Union[str, int, float]] = None,
    add_partition_for_other_values: bool=False,
    partition_clauses: List[Union[str]] = None,
    partition_min_value=None,
    partition_max_value=None,
    n_partitions: int=None,
    partition_breakpoints: List[Union[int, float, str]] = None,
    far_left_bucket: str='exclusive', #Valid values are exclusive or inclusive, or None
    far_right_bucket: str='inclusive', #Valid values are exclusive or inclusive, or None
    # far_left_boundary: str = "open", #Valid values are open, closed_inclusive, or closed_exclusive
    # far_right_boundary: str = "open", #Valid values are open, closed_inclusive, or closed_exclusive
    range_inclusive_boundary: str = "left" #Valid values are left or right
):
    partition_queries = []

    # Input Validation
    # ================
    # Make sure is select columns have been specified, none of the column names contain spaces.
    # To-Do: Is there a better way to make sure they are not aliasing or using functions, or doing calculations?
    if select_cols is not None:
        for col in select_cols:
            if " " in col:
                raise ValueError(f"Invalid column name '{col}' in select_cols. Column names must not contain spaces.")
            try:
                parsed = sqlglot.parse_one(col)
                if not isinstance(parsed, sqlglot.exp.Column):
                    raise ValueError(f"Invalid column reference '{col}' in select_cols. Only simple column names are allowed; no functions, operators, or aliases.")
            except ValueError:
                raise ValueError(f"UNable to parse column name '{col}' in select_cols")

    # Make sure that if a partition column has been specified, then sufficient accompanying arguments have also been specified.
    if partition_column:
        if partition_values is not None and partition_breakpoints is not None:
            raise ValueError("Only one of partition_values or partition_breakpoints should be provided when partition_column is set.")
        if partition_values is None and partition_breakpoints is None:
            raise ValueError("Either partition_values or partition break_points must be provided when partition_column is set.")

    # If partition breakpoints have been specified as strings, make sure those strings are ISO 8601 dates
    # To-Do: Allow datetime strings     
    if partition_breakpoints is not None:
        for bp in partition_breakpoints:
            if isinstance(bp, str):
                try:
                    datetime.strptime(bp, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Invalid ISO 8601 date format in partition_break_points: '{bp}'. Expected format is YYYY-MM-DD.")

    # If specified, check that the far boundary types are valid
    if far_left_bucket not in ['inclusive', 'exclusive']:
        raise ValueError("far_left_bucket must be 'inclusive' or 'exclusive'")
    if far_right_bucket not in ['inclusive', 'exclusive']:
        raise ValueError("far_right_bucket must be 'inclusive' or 'exclusive'")

    # Generate Base SQL Extraction Query
    # ==================================
    if select_cols is not None:
        select_exprs = [sqlglot.exp.Column(this=col) for col in select_cols]
        base_query = sqlglot.select(*select_exprs).from_(full_table_name)
    else:
        base_query = sqlglot.select("*").from_(full_table_name)

    if global_filter is not None:
        condition = sqlglot.parse_one(global_filter)
        base_query = base_query.where(condition)
    
    # Generate Partitioned Queries
    # ============================
    # 1. By explicit Where clauses
    if partition_clauses:
        for clause in partition_clauses:
            condition = sqlglot.parse_one(clause)
            partition_query = base_query.where(condition)
            partition_queries.append(partition_query.sql())
        if add_partition_for_other_values:
            parsed_clauses = [sqlglot.parse_one(clause) for clause in partition_clauses]
            negated_clauses = [sqlglot.exp.Not(this=clause) for clause in parsed_clauses]
            combined_negated_clauses = negated_clauses[0]
            for clause_expr in negated_clauses[1:]:
                combined_negated_clauses = sqlglot.exp.And(this=combined_negated_clauses, expression=clause_expr)
            other_values_partition_query = base_query.where(combined_negated_clauses)
            partition_queries.append(other_values_partition_query.sql())
    # 2. By a List of Values
    elif partition_values:
        partition_column_exp = sqlglot.exp.Column(this=partition_column)
        for value in partition_values:
            if isinstance(value, list):
                value_expressions = [sqlglot.exp.Literal.string(v) for v in value]
                condition = sqlglot.exp.In(this=partition_column_exp, expressions=value_expressions)
            else:
                value_expression = sqlglot.exp.Literal.string(value) if isinstance(value,str) else sqlglot.exp.Literal.number(value)
                condition = sqlglot.exp.EQ(this=partition_column_exp, expressions=value_expression)
            partition_query = base_query.where(condition)
            partition_queries.append(partition_query)
        if other_values_partition_query:
            flattened_values = list(chain.from_iterable(v if isinstance(v, list) else [v] for v in partition_values))
            value_expressions = [sqlglot.exp.Literal.string(v) if isinstance(v, str) else sqlglot.exp.Literal.number(v) for v in flattened_values]
            condition = sqlglot.exp.Not(sqlglot.exp.In(this=partition_column_exp, expressions=value_expressions))
            other_values_partition_query = base_query.where(condition)
            partition_queries.append(other_values_partition_query)
    # 3. By a List of Breakpoints
    elif partition_breakpoints:
        partition_column_exp = sqlglot.exp.Column(this=partition_column)

        # If only a single breakpoint was provided
        if len(partition_breakpoints) == 1:
            bp = partition_breakpoints[0]
            if isinstance(bp, str):
                if is_iso_date(bp):
                    bp_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(bp), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATE))
                elif is_iso_datetime(bp):
                    bp_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(bp), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATETIME))
            else:
                bp_exp = sqlglot.exp.Literal.number(bp)

            if range_inclusive_boundary == 'left':
                lower_condition = sqlglot.exp.LT(this=partition_column_exp, expression=bp_exp)
                upper_condition = sqlglot.exp.GTE(this=partition_column_exp, expression=bp_exp)
            elif range_inclusive_boundary == 'right':
                lower_condition = sqlglot.exp.LTE(this=partition_column_exp, expression=bp_exp)
                upper_condition = sqlglot.exp.GT(this=partition_column_exp, expression=bp_exp)
            lower_partition_query = base_query.where(lower_condition)
            upper_partition_query = base_query.where(upper_condition)
            partition_queries.append(lower_partition_query.sql())
            partition_queries.append(upper_partition_query.sql())

        # If multiple breakpoints were provided
        else:
            for i in range(max(len(partition_breakpoints)-1, 1)):
                # Defining upper and lower boundary point expressions for this iteration
                lower = partition_breakpoints[i]
                if isinstance(lower, str):
                    if is_iso_date(lower):
                        lower_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(lower), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATE))
                    elif is_iso_datetime(lower):
                        lower_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(lower), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATETIME))
                else:
                    lower_exp = sqlglot.exp.Literal.number(lower)
                upper = partition_breakpoints[i+1]
                if isinstance(upper, str):
                    if is_iso_date(lower):
                        upper_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(upper), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATE))
                    elif is_iso_datetime(upper):
                        upper_exp = sqlglot.exp.Cast(this=sqlglot.exp.Literal.string(upper), to=sqlglot.exp.DataType(this=sqlglot.exp.DataType.Type.DATETIME))
                else:
                    upper_exp = sqlglot.exp.Literal.number(upper)

                # Far Left Bucket (if required)
                if i==0 and far_left_bucket:
                    if far_left_bucket=='exclusive':
                        condition = sqlglot.exp.LT(this=partition_column_exp, expression=lower_exp)
                    elif far_left_bucket=='inclusive':
                        condition = sqlglot.exp.LTE(this=partition_column_exp, expression=lower_exp)
                    partition_query = base_query.where(condition)
                    partition_queries.append(partition_query.sql())

                # Interval Range Partitions           
                if range_inclusive_boundary=='left':
                    lower_condition = sqlglot.exp.GTE(this=partition_column_exp, expression=lower_exp)
                    if i==(len(partition_breakpoints) - 2) and far_right_bucket=='exclusive':
                        upper_condition = sqlglot.exp.LTE(this=partition_column_exp, expression=upper_exp)
                    else:
                        upper_condition = sqlglot.exp.LT(this=partition_column_exp, expression=upper_exp)
                elif range_inclusive_boundary=='right':
                    if i==0 and far_left_bucket=='exclusive':
                        lower_condition = sqlglot.exp.GTE(this=partition_column_exp, expression=lower_exp)
                    else:
                        lower_condition = sqlglot.exp.GT(this=partition_column_exp, expression=lower_exp)
                    upper_condition = sqlglot.exp.LTE(this=partition_column_exp, expression=upper_exp)
                condition = sqlglot.exp.And(lower_condition, upper_condition)
                partition_query = base_query.where(condition)
                partition_queries.append(partition_query.sql())

                # Far Right Bucket (if required)
                if i==(len(partition_breakpoints) - 2) and far_right_bucket:
                    if far_right_bucket=='inclusive':
                        condition = sqlglot.exp.GTE(this=partition_column_exp, expression=upper_exp)
                    elif far_right_bucket=='exclusive':
                        condition = sqlglot.exp.GT(this=partition_column_exp, expression=upper_exp)
                    partition_query = base_query.where(condition)
                    partition_queries.append(partition_query.sql())

    return partition_queries