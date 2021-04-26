from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo


class CosmosDatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        return [
            TableInfo(name=table["id"], type="cosmos")
            for table in cursor.get_table_list()
        ]
