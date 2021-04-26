from cosmos.client import CosmosDatabaseClient
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants


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
    def __init__(self, name, cosmos_client):
        self._name = name
        self._cosmos_client = cosmos_client

    def close(self):
        pass  # no equivalent method


class CosmosDatabaseConnection:
    def __init__(self, cosmos_client):
        self._cosmos_client = cosmos_client

    def close(self):
        pass  # no equivalent method

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self, name=None):
        return CosmosDatabaseCursor(name, self._cosmos_client)


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

    def connect(self, url, key, **kwargs):
        client = cosmos_client.CosmosClient(url, {"masterKey": key})
        return CosmosDatabaseConnection(client)
