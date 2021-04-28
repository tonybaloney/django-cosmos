import cosmos.errors as errors
from azure.cosmos.partition_key import PartitionKey

from uuid import uuid4
import re
import logging

logger = logging.getLogger(__name__)


def next_id():
    return uuid4().int >> 64


def as_result_set(result):
    return list(result.values())


class CosmosDatabaseCursor:
    def __init__(self, name, db, partition_key):
        self._name = name
        self._db = db
        self._partition_key = partition_key
        if name:
            self.set_container(name)
        else:
            self._container = None

    def close(self):
        pass  # no equivalent method

    def get_table_list(self):
        return self._db.list_containers()

    def set_container(self, name):
        self._container = self._db.get_container_client(name)

    def is_pk(self, col_name):
        return col_name == self._partition_key

    def get_items(self, table, where, params):
        self.set_container(table)
        sql = "SELECT {0}.{1} FROM {2}".format(
            table, self._partition_key, table
        )  # both quoted
        if where:
            sql += " WHERE " + where
        self.execute(sql, params)
        return [
            self._container.read_item(
                r[self._partition_key], partition_key="/{0}".format(self._partition_key)
            )
            for r in self._result
        ]

    def upsert_item(self, item):
        print("UPDATE: ", item)
        return self._container.upsert_item(item)

    def execute(self, operation, *parameters):
        logger.debug(operation, *parameters)
        if self._container is None:
            raise errors.CosmosInterfaceError("Cursor has no container")

        assert len(parameters) == 1
        # Cosmos doesn't support unnamed parameters. It requires a special list
        # of KVP dictionaries. Convert them here.
        params = []
        cleaned_sql = ""
        arg_num = 0
        cursor = 0
        for arg in re.finditer("%s", operation):
            cleaned_sql += operation[cursor : arg.start()] + "@arg{0}".format(arg_num)
            cursor = arg.end()
            params.append(
                {"name": "@arg{0}".format(arg_num), "value": parameters[0][arg_num]}
            )
            arg_num += 1
        cleaned_sql += operation[cursor:]
        print("SQL Query : ", cleaned_sql, parameters)
        self._result = self._container.query_items(
            query=cleaned_sql,
            parameters=params,
            enable_cross_partition_query=True,
            populate_query_metrics=True,
        )

    @property
    def lastrowid(self):
        return self._last_id

    @property
    def rowcount(self):
        if self._result:
            return len(list(self._result))
        else:
            return 0

    def fetchone(self):
        try:
            res = next(self._result)
            return as_result_set(res)
        except StopIteration:
            return None

    def fetchmany(self, count):
        return [as_result_set(r) for r in self._result]

    def insert_batch(self, table, pk_col, rows):
        """Insert a list of dicts to table."""

        for row in rows:
            if self._partition_key in row:
                raise errors.CosmosIntegrityError(
                    "Cannot use {0} as a column name".format(self._partition_key)
                )
            self._last_id = next_id()
            row[self._partition_key] = str(self._last_id)
            if pk_col not in row:
                row[pk_col] = self._last_id
            self._db.create_container_if_not_exists(
                id=table,
                partition_key=PartitionKey(
                    path="/{0}".format(self._partition_key), kind="Hash"
                ),
            ).create_item(
                body=row, request_options={"disableAutomaticIdGeneration": False}
            )
