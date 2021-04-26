from cosmos.client import CosmosDatabaseClient
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
from azure.cosmos.documents import ProxyConfiguration
from uuid import uuid4
import re
import logging

logger = logging.getLogger(__name__)


def next_id():
    return str(uuid4())


class CosmosWarning(Exception):
    pass


class CosmosError(Exception):
    pass


class CosmosInterfaceError(CosmosError):
    pass


class CosmosDatabaseError(CosmosError):
    pass


class CosmosDataError(Exception):
    pass


class CosmosOperationalError(CosmosDatabaseError):
    pass


class CosmosIntegrityError(CosmosDatabaseError):
    pass


class CosmosInternalError(CosmosDatabaseError):
    pass


class CosmosProgrammingError(CosmosDatabaseError):
    pass


class CosmosNotSupportedError(CosmosDatabaseError):
    pass


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

    def execute(self, operation, *parameters):
        logger.debug(operation, *parameters)
        if self._container is None:
            raise CosmosInterfaceError("Cursor has no container")

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
                {"name": "@arg{0}".format(arg_num), "value": parameters[arg_num]}
            )
            arg_num += 1
        self._result = self._container.query_items(
            query=cleaned_sql, parameters=params, enable_cross_partition_query=True
        )

    @property
    def lastrowid(self):
        return self._last_id

    def fetchone(self):
        res = next(self._result)
        return res

    def fetchmany(self, count):
        return list(self._result)

    def insert_batch(self, table, pk_col, rows):
        """Insert a list of dicts to table."""

        for row in rows:
            if self._partition_key in row:
                raise CosmosIntegrityError(
                    "Cannot use {0} as a column name".format(self._partition_key)
                )
            self._last_id = next_id()
            row[self._partition_key] = self._last_id
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


class CosmosDatabaseConnection:
    def __init__(self, db_proxy, partition_key):
        self._db = db_proxy
        self._partition_key = partition_key

    def close(self):
        pass  # no equivalent method

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self, name=None):
        return CosmosDatabaseCursor(name, self._db, self._partition_key)

    def drop_container(self, name):
        try:
            self._db.delete_container(name)
        except exceptions.CosmosResourceNotFoundError:
            pass  # already deleted

    def create_container(self, name):
        self._db.create_container_if_not_exists(
            id=name,
            partition_key=PartitionKey(
                path="/{0}".format(self._partition_key), kind="Hash"
            ),
        )


class CosmosDatabase:
    """
    PEP249 compliant wrapper around the cosmos Python API
    """

    Warning = CosmosWarning
    Error = CosmosError
    InterfaceError = CosmosInterfaceError
    DatabaseError = CosmosDatabaseError
    DataError = CosmosDataError
    OperationalError = CosmosOperationalError
    IntegrityError = CosmosIntegrityError
    InternalError = CosmosInternalError
    ProgrammingError = CosmosProgrammingError
    NotSupportedError = CosmosNotSupportedError

    def __init__(self, *args, **kwargs):
        pass

    def connect(self, url, key, database, **kwargs):
        if "PROXY" in kwargs:
            proxy = ProxyConfiguration()
            proxy.Host = kwargs.get("HOST", None)
            proxy.Port = kwargs.get("PORT", None)
        else:
            proxy = None
        if not database:
            database = "django"
        logger.debug("Connecting to {0} for database {1}.".format(url, database))
        client = cosmos_client.CosmosClient(url, key, proxy_config=proxy)
        try:
            db_proxy = client.create_database_if_not_exists(database)
            if "PARTITION_KEY" not in kwargs:
                partition_key = "id"
            else:
                partition_key = kwargs["PARTITION_KEY"]
            return CosmosDatabaseConnection(db_proxy, partition_key)
        except exceptions.CosmosHttpResponseError as e:
            raise CosmosInternalError from e
