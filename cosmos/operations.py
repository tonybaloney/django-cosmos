from django.db.backends.base.operations import BaseDatabaseOperations
import azure.cosmos


class CosmosDatabaseOperations(BaseDatabaseOperations):
    compiler_module = "cosmos.compiler"

    def quote_name(self, name: str):
        return name

    def limit_offset_sql(self, low_mark, high_mark):
        """Cosmos has a unique OFFSET/LIMIT clause"""
        limit, offset = self._get_limit_offset_params(low_mark, high_mark)
        if limit:
            return "OFFSET %d LIMIT %d" % (offset, limit)
        else:
            return ""
