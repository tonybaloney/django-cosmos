from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class CosmosDatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def create_model(self, model):
        self.connection.connection.create_container(model._meta.db_table)

        # Make M2M tables
        for field in model._meta.local_many_to_many:
            if field.remote_field.through._meta.auto_created:
                self.create_model(field.remote_field.through)

    def alter_field(self, model, old_field, new_field, strict=False):
        """
        Allow a field's type, uniqueness, nullability, default, column,
        constraints, etc. to be modified.
        `old_field` is required to compute the necessary changes.
        If `strict` is True, raise errors if the old column does not match
        `old_field` precisely.
        """
        # TODO : Logic to "upgrade" old data
        pass

    def alter_index_together(
        self,
        model,
        old_index_together,
        new_index_together,
    ):
        pass  # does nothing right now

    def alter_unique_together(
        self,
        model,
        old_unique_together,
        new_unique_together,
    ):
        pass  # does nothing right now

    def delete_model(self, model):
        self.connection.connection.drop_container(model._meta.db_table)
