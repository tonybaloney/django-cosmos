from cosmos.schema import CosmosDatabaseSchemaEditor
from cosmos.database import CosmosDatabase
from azure.cosmos import DatabaseAccount
from cosmos.validation import CosmosDatabaseValidation
from cosmos.operations import CosmosDatabaseOperations
from cosmos.introspection import CosmosDatabaseIntrospection
from cosmos.features import CosmosDatabaseFeatures
from cosmos.creation import CosmosDatabaseCreation
from cosmos.client import CosmosDatabaseClient
from django.db.backends.base.base import BaseDatabaseWrapper


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "cosmos"
    display_name = "Azure Cosmos DB"

    Database = CosmosDatabase
    client_class = CosmosDatabaseClient
    creation_class = CosmosDatabaseCreation
    features_class = CosmosDatabaseFeatures
    introspection_class = CosmosDatabaseIntrospection
    ops_class = CosmosDatabaseOperations
    validation_class = CosmosDatabaseValidation
    SchemaEditorClass = CosmosDatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check_constraints(self, table_names=None):
        """
        Check each table name in `table_names` for rows with invalid foreign
        key references. This method is intended to be used in conjunction with
        `disable_constraint_checking()` and `enable_constraint_checking()`, to
        determine if rows with invalid references were entered while constraint
        checks were off.
        """
        pass

    def set_autocommit(
        self, autocommit, force_begin_transaction_with_broken_autocommit=False
    ):
        """
        The base method actually causes a recursive loop, just set an internal flag
        """
        self._autocommit = autocommit

    def chunked_cursor(self):
        return self.cursor()

    def get_connection_params(self):
        """Return a dict of parameters suitable for get_new_connection."""
        return self.settings_dict.copy()

    def get_new_connection(self, conn_params):
        """Open a connection to the database."""
        url = conn_params.get("URL", None)
        key = conn_params.get("KEY", None)
        name = conn_params.get("NAME", "django")
        container = conn_params.get("CONTAINER", "default")
        if not url or not key:
            raise KeyError("Missing URL or KEY in database configuration")
        return self.Database().connect(url, key, name, container)

    def init_connection_state(self):
        """Initialize the database connection settings."""
        pass

    def create_cursor(self, name=None):
        """Create a cursor. Assume that a connection is established."""
        self.ensure_connection()
        return self.connection.cursor(name)

    def _set_autocommit(self, autocommit):
        """
        Backend-specific implementation to enable or disable autocommit.
        """
        self.autocommit = autocommit

    def is_usable(self):
        return True
