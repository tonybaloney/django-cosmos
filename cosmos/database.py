from cosmos.client import CosmosDatabaseClient
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey


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
    def __init__(self, name, container):
        self._name = name
        self._container = container

    def close(self):
        pass  # no equivalent method

    def get_table_list(self):
        return []

    def execute(self, operation, *parameters):
        print(operation, *parameters)
        self._container.query(operation)
        pass


class CosmosDatabaseConnection:
    def __init__(self, db_proxy, container_proxy):
        self._db = db_proxy
        self._container = container_proxy

    def close(self):
        pass  # no equivalent method

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self, name=None):
        return CosmosDatabaseCursor(name, self._container)


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

    def connect(self, url, key, database, container, **kwargs):
        client = cosmos_client.CosmosClient(url, key)
        db_proxy = client.create_database_if_not_exists(database)
        if "PARTITION_KEY" not in kwargs:
            partition_key = "id"
        else:
            partition_key = kwargs["PARTITION_KEY"]
        container_proxy = db_proxy.create_container_if_not_exists(
            id=container,
            partition_key=PartitionKey(path="/{0}".format(partition_key), kind="Hash"),
        )
        return CosmosDatabaseConnection(db_proxy, container_proxy)
