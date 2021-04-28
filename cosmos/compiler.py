from django.db import NotSupportedError
from django.db.transaction import TransactionManagementError
from django.db.models.sql import compiler
from django.db.models.sql.constants import (
    CURSOR,
    GET_ITERATOR_CHUNK_SIZE,
    MULTI,
    NO_RESULTS,
    ORDER_DIR,
    SINGLE,
)
from django.core.exceptions import EmptyResultSet
from django.db.models.sql.constants import INNER, LOUTER, ORDER_DIR, SINGLE
from django.db.models.sql.datastructures import Join


class SQLCompiler(compiler.SQLCompiler):
    def _compile_join(self, compiler, join, connection):
        """
        Generate the full
           LEFT OUTER JOIN sometable ON sometable.somecol = othertable.othercol, params
        clause for this join.
        """
        join_conditions = []
        params = []
        qn = compiler.quote_name_unless_alias
        qn2 = connection.ops.quote_name

        # Cosmos only has inner joins
        if join.join_type == INNER:
            join.join_type = "JOIN"
        else:
            raise NotSupportedError("Does not support {0}".format(join.join_type))

        # Add a join condition for each pair of joining columns.
        for lhs_col, rhs_col in join.join_cols:
            join_conditions.append(
                "%s.%s"
                % (
                    qn(join.parent_alias),
                    qn2(lhs_col),
                    # qn(join.table_alias),
                    # qn2(rhs_col),
                )
            )

        # Add a single condition inside parentheses for whatever
        # get_extra_restriction() returns.
        extra_cond = join.join_field.get_extra_restriction(
            compiler.query.where_class, join.table_alias, join.parent_alias
        )
        if extra_cond:
            extra_sql, extra_params = compiler.compile(extra_cond)
            join_conditions.append("(%s)" % extra_sql)
            params.extend(extra_params)
        if join.filtered_relation:
            extra_sql, extra_params = compiler.compile(join.filtered_relation)
            if extra_sql:
                join_conditions.append("(%s)" % extra_sql)
                params.extend(extra_params)
        if not join_conditions:
            # This might be a rel on the other end of an actual declared field.
            declared_field = getattr(join.join_field, "field", join.join_field)
            raise ValueError(
                "Join generated an empty ON clause. %s did not yield either "
                "joining columns or extra restrictions." % declared_field.__class__
            )
        on_clause_sql = ", ".join(join_conditions)
        alias_str = (
            "" if join.table_alias == join.table_name else (" %s" % join.table_alias)
        )
        sql = "%s %s%s IN %s" % (
            join.join_type,
            qn(join.table_name),
            alias_str,
            on_clause_sql,
        )
        return sql, params

    def compile(self, node):
        if isinstance(node, Join):
            return self._compile_join(self, node, self.connection)
        vendor_impl = getattr(node, "as_" + self.connection.vendor, None)
        if vendor_impl:
            sql, params = vendor_impl(self, self.connection)
        else:
            sql, params = node.as_sql(self, self.connection)
        return sql, params

    def pre_sql_setup(self):
        """
        Do any necessary class setup immediately prior to producing SQL. This
        is for things that can't necessarily be done in __init__ because we
        might not have all the pieces in place at that time.
        """
        self.setup_query()
        order_by = []
        # else:
        #     order_by = self.get_order_by()
        self.where, self.having = self.query.where.split_having()
        extra_select = self.get_extra_select(order_by, self.select)
        self.has_extra_select = bool(extra_select)
        group_by = self.get_group_by(self.select + extra_select, order_by)
        return extra_select, order_by, group_by

    def execute_sql(
        self, result_type=MULTI, chunked_fetch=False, chunk_size=GET_ITERATOR_CHUNK_SIZE
    ):
        """
        Run the query against the database and return the result(s). The
        return value is a single data item if result_type is SINGLE, or an
        iterator over the results if the result_type is MULTI.

        result_type is either MULTI (use fetchmany() to retrieve all rows),
        SINGLE (only retrieve a single row), or None. In this last case, the
        cursor is returned if any query is executed, since it's used by
        subclasses such as InsertQuery). It's possible, however, that no query
        is needed, as the filters describe an empty set. In that case, None is
        returned, to avoid any unnecessary database interaction.
        """
        result_type = result_type or NO_RESULTS
        try:
            sql, params = self.as_sql()
            if not sql:
                raise EmptyResultSet
        except EmptyResultSet:
            if result_type == MULTI:
                return iter([])
            else:
                return
        if chunked_fetch:
            cursor = self.connection.chunked_cursor()
            cursor.set_container(self.query.get_meta().db_table)
        else:
            cursor = self.connection.cursor()
            cursor.set_container(self.query.get_meta().db_table)
        try:
            cursor.execute(sql, params)
        except Exception:
            # Might fail for server-side cursors (e.g. connection closed)
            cursor.close()
            raise

        if result_type == CURSOR:
            # Give the caller the cursor to process and close.
            return cursor
        if result_type == SINGLE:
            try:
                val = cursor.fetchone()
                if val:
                    return val[0 : self.col_count]
                return val
            finally:
                # done with the cursor
                cursor.close()
        if result_type == NO_RESULTS:
            cursor.close()
            return

        result = compiler.cursor_iter(
            cursor,
            self.connection.features.empty_fetchmany_value,
            self.col_count if self.has_extra_select else None,
            chunk_size,
        )
        if not chunked_fetch or not self.connection.features.can_use_chunked_reads:
            try:
                # If we are using non-chunked reads, we return the same data
                # structure as normally, but ensure it is all read into memory
                # before going any further. Use chunked_fetch if requested,
                # unless the database doesn't support it.
                return list(result)
            finally:
                # done with the cursor
                cursor.close()
        return result


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def execute_sql(self, returning_fields=None):
        assert not (
            returning_fields
            and len(self.query.objs) != 1
            and not self.connection.features.can_return_rows_from_bulk_insert
        )
        self.returning_fields = returning_fields
        with self.connection.cursor() as cursor:
            batch = self.as_batch()
            cursor.insert_batch(
                self.query.get_meta().db_table, self.query.get_meta().pk.column, batch
            )
            if not self.returning_fields:
                return []
            if (
                self.connection.features.can_return_rows_from_bulk_insert
                and len(self.query.objs) > 1
            ):
                return self.connection.ops.fetch_returned_insert_rows(cursor)
            if self.connection.features.can_return_columns_from_insert:
                assert len(self.query.objs) == 1
                return [
                    self.connection.ops.fetch_returned_insert_columns(
                        cursor, self.returning_params
                    )
                ]
            return [
                (
                    self.connection.ops.last_insert_id(
                        cursor,
                        self.query.get_meta().db_table,
                        self.query.get_meta().pk.column,
                    ),
                )
            ]

    def as_batch(self):
        """
        Return the values in the query as a list of rows to be entered into the DB
        """
        opts = self.query.get_meta()
        fields = self.query.fields or [opts.pk]

        if self.query.fields:
            value_rows = [
                [
                    self.prepare_value(field, self.pre_save_val(field, obj))
                    for field in fields
                ]
                for obj in self.query.objs
            ]
        else:
            # An empty object.
            value_rows = [
                [self.connection.ops.pk_default_value()] for _ in self.query.objs
            ]
            fields = [None]

        rows = [
            {field.column: row[i] for i, field in enumerate(fields)}
            for row in value_rows
        ]
        return rows

    def field_as_sql(self, field, val):
        """
        Take a field and a value intended to be saved on that field, and
        return placeholder SQL and accompanying params. Check for raw values,
        expressions, and fields with get_placeholder() defined in that order.

        When field is None, consider the value raw and use it as the
        placeholder, with no corresponding parameters returned.

        returns a tuple (sql, params)
        """
        return super().field_as_sql(field, val)


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_update_batch(self, cursor):
        """
        Update the items in the database
        """
        self.pre_sql_setup()
        if not self.query.values:
            return []
        qn = self.quote_name_unless_alias
        table = self.query.base_table

        # Convert partition key params into strings.
        for col in self.query.where.children:
            if cursor.is_pk(col.lhs.target.column):
                if isinstance(col.rhs, int):
                    col.rhs = str(col.rhs)
        # Get the items first
        where, params = self.compile(self.query.where)
        
        marked_updates = cursor.get_items(qn(table), where, params)

        for item in marked_updates:
            values, update_params = [], []
            for field, model, val in self.query.values:
                if hasattr(val, "resolve_expression"):
                    val = val.resolve_expression(
                        self.query, allow_joins=False, for_save=True
                    )
                    if val.contains_aggregate:
                        raise FieldError(
                            "Aggregate functions are not allowed in this query "
                            "(%s=%r)." % (field.name, val)
                        )
                    if val.contains_over_clause:
                        raise FieldError(
                            "Window expressions are not allowed in this query "
                            "(%s=%r)." % (field.name, val)
                        )
                elif hasattr(val, "prepare_database_save"):
                    if field.remote_field:
                        val = field.get_db_prep_save(
                            val.prepare_database_save(field),
                            connection=self.connection,
                        )
                    else:
                        raise TypeError(
                            "Tried to update field %s with a model instance, %r. "
                            "Use a value compatible with %s."
                            % (field, val, field.__class__.__name__)
                        )
                else:
                    val = field.get_db_prep_save(val, connection=self.connection)

                # Getting the placeholder for the field.
                if hasattr(field, "get_placeholder"):
                    placeholder = field.get_placeholder(val, self, self.connection)
                else:
                    placeholder = "%s"
                name = field.column
                if hasattr(val, "as_sql"):
                    sql, params = self.compile(val)
                    item[qn(name)] = placeholder % sql
                    update_params.extend(params)
                elif val is not None:
                    item[qn(name)] = placeholder
                else:
                    item[qn(name)] = None
            cursor.upsert_item(item)
        return len(marked_updates)

    def execute_sql(self, result_type):
        """
        Execute the specified update. Return the number of rows affected by
        the primary update query. The "primary update query" is the first
        non-empty query that is executed. Row counts for any subsequent,
        related queries are not available.
        """
        with self.connection.cursor() as cursor:
            rows = self.as_update_batch(cursor)  # number of impacted rows
        is_empty = rows == 0
        for query in self.query.get_related_updates():
            aux_rows = query.get_compiler(self.using).execute_sql(result_type)
            if is_empty and aux_rows:
                rows = aux_rows
                is_empty = False
        return rows


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
