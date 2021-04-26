from django.db.backends.base.operations import BaseDatabaseOperations
import azure.cosmos


class CosmosDatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name: str):
        return name
