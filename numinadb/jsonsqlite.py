#
# from https://avacariu.me/articles/2016/compiling-json-as-text-for-sqlite-with-sqlalchemy


import sqlalchemy.types as types
import json


class StringyJSON(types.TypeDecorator):
    """Stores and retrieves JSON as TEXT."""

    impl = types.TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


# TypeEngine.with_variant says "use StringyJSON instead when
# connecting to 'sqlite'"
MagicJSON = types.JSON().with_variant(StringyJSON, 'sqlite')
