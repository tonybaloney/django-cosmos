from cosmos.client import CosmosDatabaseClient
from cosmos.cursor import CosmosDatabaseCursor
import cosmos.errors as errors
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
from azure.cosmos.documents import ProxyConfiguration

import logging

logger = logging.getLogger(__name__)


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

    Warning = errors.CosmosWarning
    Error = errors.CosmosError
    InterfaceError = errors.CosmosInterfaceError
    DatabaseError = errors.CosmosDatabaseError
    DataError = errors.CosmosDataError
    OperationalError = errors.CosmosOperationalError
    IntegrityError = errors.CosmosIntegrityError
    InternalError = errors.CosmosInternalError
    ProgrammingError = errors.CosmosProgrammingError
    NotSupportedError = errors.CosmosNotSupportedError

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
            raise errors.CosmosInternalError from e
