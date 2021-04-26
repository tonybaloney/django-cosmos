from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo


class CosmosDatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        breakpoint()
        pass
