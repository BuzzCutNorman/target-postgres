"""postgres target sink class, which handles writing streams."""

from __future__ import annotations

from base64 import b64decode
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, Optional, cast

import sqlalchemy as sa
from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import SQLSink
from sqlalchemy import MetaData, Table, engine_from_config, exc, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.url import URL

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


MSSQL_BIGINT_MIN: int = -9223372036854775808
MSSQL_BIGINT_MAX: int = 9223372036854775807
MSSQL_INT_MIN: int = -2147483648
MSSQL_INT_MAX: int = 2147483647
MSSQL_SMALLINT_MIN: int = -32768
MSSQL_SMALLINT_MAX: int = 32767
MSSQL_TINYINT_MIN: int = 0
MSSQL_TINYINT_MAX: int = 255
MSSQL_MONEY_MIN:Decimal = Decimal("-922337203685477.6")
MSSQL_MONEY_MAX:Decimal = Decimal("922337203685477.6")
MSSQL_SMALLMONEY_MIN:Decimal = Decimal("-214748.3648")
MSSQL_SMALLMONEY_MAX:Decimal = Decimal("214748.3647")
MSSQL_FLOAT_MIN:Decimal = Decimal("-1.79e308")
MSSQL_FLOAT_MAX:Decimal = Decimal("1.79e308")
MSSQL_REAL_MIN:Decimal = Decimal("-3.40e38")
MSSQL_REAL_MAX:Decimal = Decimal("3.40e38")


class PostgresConnector(SQLConnector):
    """The connector for postgres.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_overwrite: bool = False  # Whether overwrite load method is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    @contextmanager
    def _connect(self) -> Iterator[sa.engine.Connection]:
        with self._engine.connect() as conn:
            yield conn

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.
        """
        url_drivername = f"{config.get('dialect')}+{config.get('driver_type')}"

        config_url = URL.create(
            url_drivername,
            config["user"],
            config["password"],
            host=config["host"],
            database=config["database"]
        )

        if "port" in config:
            config_url = config_url.set(port=config["port"])

        if "sqlalchemy_url_query" in config:
            config_url = config_url.update_query_dict(config["sqlalchemy_url_query"])

        return (config_url)

    def create_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False",
            f"{eng_prefix}json_serializer": self.serialize_json,
            f"{eng_prefix}json_deserializer": self.deserialize_json,
        }

        if self.config.get("sqlalchemy_eng_params"):
            for key, value in self.config["sqlalchemy_eng_params"].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return engine_from_config(eng_config, prefix=eng_prefix)

    def to_sql_type(self, jsonschema_type: dict) -> None:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        msg = f"json schema type: {jsonschema_type}"
        self.logger.info(msg)
        if self.config.get("hd_jsonschema_types", False):
            return self.hd_to_sql_type(jsonschema_type)
        return self.org_to_sql_type(jsonschema_type)

    @staticmethod
    def org_to_sql_type(jsonschema_type: dict) -> sa.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        if jsonschema_type.get("format") == "date-time":
            return cast(types.TypeEngine, sa.types.TIMESTAMP())

        return SQLConnector.to_sql_type(jsonschema_type)

    @staticmethod
    def hd_to_sql_type(jsonschema_type: dict) -> types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        # JSON Strings to Postgres
        if "string" in jsonschema_type.get("type"):
            if jsonschema_type.get("format") == "date":
                return cast(sa.types.TypeEngine, postgresql.DATE())
            if jsonschema_type.get("format") == "time":
                return cast(sa.types.TypeEngine, postgresql.TIME())
            if jsonschema_type.get("format") == "date-time":
                return cast(sa.types.TypeEngine, postgresql.TIMESTAMP())
            if jsonschema_type.get("format") == "uuid":
                return cast(sa.types.TypeEngine, postgresql.UUID())
            if jsonschema_type.get("contentMediaType") == "application/xml":
                return cast(sa.types.TypeEngine, postgresql.TEXT())
            length: int = jsonschema_type.get("maxLength")
            if jsonschema_type.get("contentEncoding") == "base64":
                if length:
                    return cast(sa.types.TypeEngine, postgresql.BYTEA(length=length))
                return cast(sa.types.TypeEngine, postgresql.BYTEA())
            if length:
                return cast(sa.types.TypeEngine,  postgresql.VARCHAR(length=length))
            return cast(sa.types.TypeEngine, postgresql.VARCHAR())

        # JSON Boolean to Postgres
        if "boolean" in jsonschema_type.get("type"):
            return cast(types.TypeEngine, postgresql.BOOLEAN())

        # JSON Integers to Postgres
        if "integer" in jsonschema_type.get("type"):
            minimum = jsonschema_type.get("minimum")
            maximum = jsonschema_type.get("maximum")
            if (minimum == MSSQL_BIGINT_MIN) and (maximum == MSSQL_BIGINT_MAX):
                return cast(sa.types.TypeEngine, postgresql.BIGINT())
            if (minimum == MSSQL_INT_MIN) and (maximum == MSSQL_INT_MAX):
                return cast(sa.types.TypeEngine, postgresql.INTEGER())
            if (minimum == MSSQL_SMALLINT_MIN) and (maximum == MSSQL_SMALLINT_MAX):
                return cast(sa.types.TypeEngine, postgresql.SMALLINT())
            if (minimum == MSSQL_TINYINT_MIN) and (maximum == MSSQL_TINYINT_MAX):
                # This is a MSSQL only DataType of TINYINT
                return cast(sa.types.TypeEngine, postgresql.SMALLINT())
            precision = str(maximum).count("9")
            return cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=0))

        # JSON Numbers to Postgres
        if "number" in jsonschema_type.get("type"):
            minimum = jsonschema_type.get("minimum")
            maximum = jsonschema_type.get("maximum")
            # There is something that is traucating and rounding this number
            # if (minimum == -922337203685477.5808) and (maximum == 922337203685477.5807):
            if (minimum == MSSQL_MONEY_MIN) and (maximum == MSSQL_MONEY_MAX):
                return cast(sa.types.TypeEngine, postgresql.MONEY())
            if (minimum == MSSQL_SMALLMONEY_MIN) and (maximum == MSSQL_SMALLMONEY_MAX):
                # This is a MSSQL only DataType of SMALLMONEY
                return cast(sa.types.TypeEngine, postgresql.MONEY())
            if (minimum == MSSQL_FLOAT_MIN) and (maximum == MSSQL_FLOAT_MAX):
                return cast(sa.types.TypeEngine, postgresql.FLOAT())
            if (minimum == MSSQL_REAL_MIN) and (maximum == MSSQL_REAL_MAX):
                return cast(sa.types.TypeEngine, postgresql.REAL())
            # Python will start using scientific notition for float values.
            # A check for 'e+' in the string of the value is what I key on.
            # If it is no present we can count the number of '9' chars.
            # If it is present we need to do a little more to translate.
            if "e+" not in str(maximum).lower():
                precision = str(maximum).count('9')
                scale = precision - str(maximum).rfind('.')
                return cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))
            precision_start = str(maximum).rfind('+')
            precision = int(str(maximum)[precision_start:])
            scale_start = str(maximum).find('.') + 1
            scale_end = str(maximum).lower().find('e')
            scale = scale_end - scale_start
            return cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))

        return SQLConnector.to_sql_type(jsonschema_type)

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            msg = "Temporary tables are not supported."
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sa.MetaData(schema=schema_name)
        columns: list[sa.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            if property_name in primary_keys:
                columns.append(
                    sa.Column(
                        property_name,
                        self.to_sql_type(property_jsonschema),
                        primary_key=True,
                        autoincrement=False,
                    )
                )
            else:
                columns.append(
                    sa.Column(
                        property_name,
                        self.to_sql_type(property_jsonschema),
                    ),
                )

        sa.Table(table_name, meta, *columns).create(self._engine)


class PostgresSink(SQLSink):
    """postgres target sink class."""

    connector_class = PostgresConnector

    _target_table: Table = None

    @property
    def target_table(self) -> Table:
        """Return the targeted table or `None` if not assigned yet.

        Returns:
            The target table object.
        """
        return self._target_table

    def conform_name(self, name: str, object_type: str | None = None) -> str:
        """Conform a stream property name to one suitable for the target system.

        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.

        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.


        Returns:
            The name transformed to snake case.
        """
        # # strip non-alphanumeric characters, keeping - . _ and spaces
        # name = re.sub(r"[^a-zA-Z0-9_\-\.\s]", "", name)
        # # convert to snakecase
        # name = snakecase(name)
        name = name.lower()
        # # replace leading digit
        # return replace_leading_digit(name)
        return name
        # return super().conform_name(name)

    def preprocess_record(self, record: dict, context: dict) -> dict:  # noqa: ARG002
        """Process incoming record and return a modified result.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            A new, processed record.
        """
        # Get the Stream Properties Dictornary from the Schema
        properties: dict = self.schema.get("properties")

        for key, value in record.items():
            if value is not None:
                # Get the Item/Column property
                property_schema: dict = properties.get(key)
                # PostgreSQL does not filter out Null characters
                # presence of these characters will cause
                # the target to fail out
                if isinstance(value, str) and "\x00" in value:
                    record.update({key: value.replace("\x00", "")})
                    self.logger.info("Removed Null Character(s) From a Record")
                # Decode base64 binary fields in record
                if property_schema.get("contentEncoding") == "base64":
                    record.update({key: b64decode(value)})

        return record

    def set_target_table(self, full_table_name: str) -> None:
        """Populates the property _target_table."""
        # We need to grab the schema_name and table_name
        # for the Table class instance
        _, schema_name, table_name = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)

        # You also need a blank MetaData instance
        # for the Table class instance
        meta = MetaData()

        # This is the Table instance that will autoload
        # all the info about the table from the target server
        table: Table = Table(table_name, meta, autoload_with=self.connector._engine, schema=schema_name)

        self._target_table = table

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        if self.target_table is None:
            self.set_target_table(full_table_name)

        conformed_records = (
            [self.conform_record(record) for record in records]
            if isinstance(records, list)
            else (self.conform_record(record) for record in records)
        )

        # This is a insert based off SQLA example
        # https://docs.sqlalchemy.org/en/20/dialects/mssql.html#insert-behavior
        rowcount: int = 0
        try:
            with self.connector._connect() as conn, conn.begin():  # noqa: SLF001
                result:sa.CursorResult = conn.execute(self.target_table.insert(), conformed_records)
            rowcount = result.rowcount
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            self.logger.info(error)

        return rowcount
