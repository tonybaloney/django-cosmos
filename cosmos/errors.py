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
