"""postgres target sink class, which handles writing streams."""

from __future__ import annotations
from base64 import b64decode
from typing import Any, Dict, cast, Iterable, Iterator, Optional
from contextlib import contextmanager

import sqlalchemy
from sqlalchemy import Table, MetaData, exc, types, engine_from_config
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from singer_sdk.sinks import SQLSink
from singer_sdk.connectors import SQLConnector


class postgresConnector(SQLConnector):
    """The connector for postgres.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    @contextmanager
    def _connect(self) -> Iterator[sqlalchemy.engine.Connection]:
        with self._engine.connect() as conn:
            yield conn

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.
        """
        if config['dialect'] == "postgresql":
            url_drivername: str = config['dialect']
        else:
            cls.logger.error("Invalid dialect given")
            exit(1)

        if config['driver_type'] in ["psycopg2", "pg8000", "asyncpg", "psycopg2cffi", "pypostgresql", "pygresql"]:
            url_drivername += f"+{config['driver_type']}"
        else:
            cls.logger.error("Invalid driver_type given")
            exit(1)

        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host=config['host'],
            database=config['database']
        )

        if 'port' in config:
            config_url = config_url.set(port=config['port'])

        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])

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
            f"{eng_prefix}echo": "False"
        }

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
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
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_sql_type(jsonschema_type)
        else:
            return self.org_to_sql_type(jsonschema_type)

    @staticmethod
    def org_to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
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
        # Optionally, add custom logic before calling the super().
        # You may delete this method if overrides are not needed.
        # logger = logging.getLogger("sqlconnector")
        if jsonschema_type.get('format') == 'date-time':
            return cast(types.TypeEngine, sqlalchemy.types.TIMESTAMP())

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
        if 'string' in jsonschema_type.get('type'):
            if jsonschema_type.get("format") == "date":
                return cast(sqlalchemy.types.TypeEngine, postgresql.DATE())
            if jsonschema_type.get("format") == "time":
                return cast(sqlalchemy.types.TypeEngine, postgresql.TIME())
            if jsonschema_type.get("format") == "date-time":
                return cast(sqlalchemy.types.TypeEngine, postgresql.TIMESTAMP())
            if jsonschema_type.get("format") == "uuid":
                return cast(sqlalchemy.types.TypeEngine, postgresql.UUID())
            if jsonschema_type.get("contentMediaType") == "application/xml":
                return cast(sqlalchemy.types.TypeEngine, postgresql.TEXT())
            length: int = jsonschema_type.get('maxLength')
            if jsonschema_type.get("contentEncoding") == "base64":
                if length:
                    return cast(sqlalchemy.types.TypeEngine, postgresql.BYTEA(length=length))
                else:
                    return cast(sqlalchemy.types.TypeEngine, postgresql.BYTEA())
            if length:
                return cast(sqlalchemy.types.TypeEngine,  postgresql.VARCHAR(length=length))
            else:
                return cast(sqlalchemy.types.TypeEngine, postgresql.VARCHAR())

        # JSON Boolean to Postgres
        if 'boolean' in jsonschema_type.get('type'):
            return cast(types.TypeEngine, postgresql.BOOLEAN())

        # JSON Integers to Postgres
        if 'integer' in jsonschema_type.get('type'):
            minimum = jsonschema_type.get('minimum')
            maximum = jsonschema_type.get('maximum')
            if (minimum == -9223372036854775808) and (maximum == 9223372036854775807):
                return cast(sqlalchemy.types.TypeEngine, postgresql.BIGINT())
            elif (minimum == -2147483648) and (maximum == 2147483647):
                return cast(sqlalchemy.types.TypeEngine, postgresql.INTEGER())
            elif (minimum == -32768) and (maximum == 32767):
                return cast(sqlalchemy.types.TypeEngine, postgresql.SMALLINT())
            elif (minimum == 0) and (maximum == 255):
                # This is a MSSQL only DataType of TINYINT
                return cast(sqlalchemy.types.TypeEngine, postgresql.SMALLINT())
            else:
                precision = str(maximum).count('9')
                return cast(sqlalchemy.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=0))

        # JSON Numbers to Postgres
        if 'number' in jsonschema_type.get('type'):
            minimum = jsonschema_type.get('minimum')
            maximum = jsonschema_type.get('maximum')
            # There is something that is traucating and rounding this number
            # if (minimum == -922337203685477.5808) and (maximum == 922337203685477.5807):
            if (minimum == -922337203685477.6) and (maximum == 922337203685477.6):
                return cast(sqlalchemy.types.TypeEngine, postgresql.MONEY())
            elif (minimum == -214748.3648) and (maximum == 214748.3647):
                # This is a MSSQL only DataType of SMALLMONEY
                return cast(sqlalchemy.types.TypeEngine, postgresql.MONEY())
            elif (minimum == -1.79e308) and (maximum == 1.79e308):
                return cast(sqlalchemy.types.TypeEngine, postgresql.FLOAT())
            elif (minimum == -3.40e38) and (maximum == 3.40e38):
                return cast(sqlalchemy.types.TypeEngine, postgresql.REAL())
            else:
                # Python will start using scientific notition for float values.
                # A check for 'e+' in the string of the value is what I key on.
                # If it is no present we can count the number of '9' chars.
                # If it is present we need to do a little more to translate.
                if 'e+' not in str(maximum):
                    precision = str(maximum).count('9')
                    scale = precision - str(maximum).rfind('.')
                    return cast(sqlalchemy.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))
                else:
                    precision_start = str(maximum).rfind('+')
                    precision = int(str(maximum)[precision_start:])
                    scale_start = str(maximum).find('.') + 1
                    scale_end = str(maximum).find('e')
                    scale = scale_end - scale_start
                    return cast(sqlalchemy.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))

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
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                    autoincrement=False,
                ),
            )

        empty_table = sqlalchemy.Table(table_name, meta, *columns)
        empty_table.create(self._engine)


class postgresSink(SQLSink):
    """postgres target sink class."""

    connector_class = postgresConnector

    _target_table: Table = None

    @property
    def target_table(self):
        return self._target_table

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
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
        properties: dict = self.schema.get('properties')

        for key, value in record.items():
            if value is not None:
                # Get the Item/Column property
                property_schema: dict = properties.get(key)
                # PostgreSQL does not filter out Null characters
                # presence of these characters will cause
                # the target to fail out
                if isinstance(value, str) and '\x00' in value:
                    record.update({key: value.replace('\x00', '')})
                    self.logger.info("Removed Null Character(s) From a Record")
                # Decode base64 binary fields in record
                if property_schema.get('contentEncoding') == 'base64':
                    record.update({key: b64decode(value)})

        return record

    def set_target_table(self, full_table_name: str):
        # We need to grab the schema_name and table_name
        # for the Table class instance
        _, schema_name, table_name = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)

        # You also need a blank MetaData instance
        # for the Table class instance
        meta = MetaData()

        # This is the Table instance that will autoload
        # all the info about the table from the target server
        table: Table = Table(table_name, meta, autoload=True, autoload_with=self.connector._engine, schema=schema_name)

        self._target_table = table

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
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
        try:
            with self.connector._connect() as conn:
                with conn.begin():
                    conn.execute(
                        self.target_table.insert(),
                        conformed_records,
                    )
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.
