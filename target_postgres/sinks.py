"""postgres target sink class, which handles writing streams."""

from __future__ import annotations

import asyncio
import typing as t
from base64 import b64decode
from contextlib import contextmanager
from decimal import Decimal
from gzip import GzipFile
from gzip import open as gzip_open
from pathlib import Path

import sqlalchemy as sa
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchFileFormat,
    StorageTarget,
)
from singer_sdk.helpers.capabilities import TargetLoadMethods
from singer_sdk.sinks import SQLSink
from sqlalchemy import exc
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DropTable

from .json import deserialize_json, serialize_json

if t.TYPE_CHECKING:
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

# This was added to allow load_method: overwrite to delete all
# of the table's dependent items. Views mainly that require the
# table to be present.  I took the code from these sources:
#
# https://stackoverflow.com/questions/38678336/sqlalchemy-how-to-implement-drop-table-cascade#
#
# https://docs.sqlalchemy.org/en/20/core/compiler.html#changing-the-default-compilation-of-existing-constructs
#
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs) -> str:
    return f"{compiler.visit_drop_table(element)} CASCADE"


class PostgresConnector(SQLConnector):
    """The connector for postgres.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_overwrite: bool = True  # Whether overwrite load method is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
        ) -> None:
        """Class Default Init."""
        self.deserialize_json = deserialize_json
        self.serialize_json = serialize_json

        super().__init__(config, sqlalchemy_url)

    @contextmanager
    def _connect(self) -> t.Iterator[sa.engine.Connection]:
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

        config_url = sa.URL.create(
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

        return sa.engine_from_config(eng_config, prefix=eng_prefix)

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
            return t.cast(sa.types.TypeEngine, sa.types.TIMESTAMP())

        return SQLConnector.to_sql_type(jsonschema_type)

    @staticmethod
    def hd_to_sql_type(jsonschema_type: dict) -> sa.types.TypeEngine:
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
                return t.cast(sa.types.TypeEngine, postgresql.DATE())
            if jsonschema_type.get("format") == "time":
                return t.cast(sa.types.TypeEngine, postgresql.TIME())
            if jsonschema_type.get("format") == "date-time":
                return t.cast(sa.types.TypeEngine, postgresql.TIMESTAMP())
            if jsonschema_type.get("format") == "uuid":
                return t.cast(sa.types.TypeEngine, postgresql.UUID())
            if jsonschema_type.get("contentMediaType") == "application/xml":
                return t.cast(sa.types.TypeEngine, postgresql.TEXT())
            length: int = jsonschema_type.get("maxLength")
            if jsonschema_type.get("contentEncoding") == "base64":
                if length:
                    return t.cast(sa.types.TypeEngine, postgresql.BYTEA(length=length))
                return t.cast(sa.types.TypeEngine, postgresql.BYTEA())
            if length:
                return t.cast(sa.types.TypeEngine,  postgresql.VARCHAR(length=length))
            return t.cast(sa.types.TypeEngine, postgresql.VARCHAR())

        # JSON Boolean to Postgres
        if "boolean" in jsonschema_type.get("type"):
            return t.cast(sa.types.TypeEngine, postgresql.BOOLEAN())

        # JSON Integers to Postgres
        if "integer" in jsonschema_type.get("type"):
            minimum = jsonschema_type.get("minimum")
            maximum = jsonschema_type.get("maximum")
            if (minimum == MSSQL_BIGINT_MIN) and (maximum == MSSQL_BIGINT_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.BIGINT())
            if (minimum == MSSQL_INT_MIN) and (maximum == MSSQL_INT_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.INTEGER())
            if (minimum == MSSQL_SMALLINT_MIN) and (maximum == MSSQL_SMALLINT_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.SMALLINT())
            if (minimum == MSSQL_TINYINT_MIN) and (maximum == MSSQL_TINYINT_MAX):
                # This is a MSSQL only DataType of TINYINT
                return t.cast(sa.types.TypeEngine, postgresql.SMALLINT())
            precision = str(maximum).count("9")
            return t.cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=0))

        # JSON Numbers to Postgres
        if "number" in jsonschema_type.get("type"):
            minimum = jsonschema_type.get("minimum")
            maximum = jsonschema_type.get("maximum")
            # There is something that is traucating and rounding this number
            # if (minimum == -922337203685477.5808) and (maximum == 922337203685477.5807):
            if (minimum == MSSQL_MONEY_MIN) and (maximum == MSSQL_MONEY_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.MONEY())
            if (minimum == MSSQL_SMALLMONEY_MIN) and (maximum == MSSQL_SMALLMONEY_MAX):
                # This is a MSSQL only DataType of SMALLMONEY
                return t.cast(sa.types.TypeEngine, postgresql.MONEY())
            if (minimum == MSSQL_FLOAT_MIN) and (maximum == MSSQL_FLOAT_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.FLOAT())
            if (minimum == MSSQL_REAL_MIN) and (maximum == MSSQL_REAL_MAX):
                return t.cast(sa.types.TypeEngine, postgresql.REAL())
            # Python will start using scientific notition for float values.
            # A check for 'e+' in the string of the value is what I key on.
            # If it is no present we can count the number of '9' chars.
            # If it is present we need to do a little more to translate.
            if "e+" not in str(maximum).lower():
                precision = str(maximum).count('9')
                scale = precision - str(maximum).rfind('.')
                return t.cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))
            precision_start = str(maximum).rfind('+')
            precision = int(str(maximum)[precision_start:])
            scale_start = str(maximum).find('.') + 1
            scale_end = str(maximum).lower().find('e')
            scale = scale_end - scale_start
            return t.cast(sa.types.TypeEngine, postgresql.NUMERIC(precision=precision, scale=scale))

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

    _target_table: sa.Table = None
    _insert_statement: postgresql.Insert = None

    @property
    def target_table(self) -> sa.Table:
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

        null_characters_removed: bool = False

        for key, value in record.items():
            if value is not None:
                # Get the Item/Column property
                property_schema: dict = properties.get(key)
                # PostgreSQL does not filter out Null characters
                # presence of these characters will cause
                # the target to fail out
                if isinstance(value, str) and "\x00" in value:
                    record.update({key: value.replace("\x00", "")})
                    null_characters_removed = True
                # Decode base64 binary fields in record
                if property_schema.get("contentEncoding") == "base64":
                    record.update({key: b64decode(value)})

        if null_characters_removed:
            self.logger.info("Removed Null Character(s) From a Record")

        return record

    def process_batch_line(self, line) -> dict:
        """Process a batch file record.

        This processing allows for datetimes and other types to be
        handled the same as being read from the stdout.

        Args:
            line: The batch file line to be processed.
        """
        record = self.preprocess_record(deserialize_json(line),{})
        self._parse_timestamps_in_record(
            record=record,
            schema=self.schema,
            treatment=self.datetime_error_treatment,
        )
        return record

    async def cleanup_batch_files(self, file_path: Path) -> None:
        """ASYNC function to cleanup batch files after ingestion.

        Args:
            file_path: The Path object to the file.
        """
        file_path.unlink()

    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: t.Sequence[str],
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.

        Raises:
            NotImplementedError: If the batch file encoding is not supported.
        """
        file: GzipFile | t.IO
        storage: StorageTarget | None = None

        for path in files:
            file_path = Path(path.replace("file://",""))
            head, tail = StorageTarget.split_url(path)


            if self.batch_config:
                storage = self.batch_config.storage
            else:
                storage = StorageTarget.from_url(head)

            if encoding.format == BatchFileFormat.JSONL:
                with storage.fs(create=False) as batch_fs, batch_fs.open(
                    tail,
                    mode="rb",
                ) as file:
                    context_file = (
                        gzip_open(file) if encoding.compression == "gzip" else file
                    )
                    context = {
                        "records": [self.process_batch_line(line) for line in context_file]  # type: ignore[attr-defined]
                    }
                    self.process_batch(context)
            else:
                msg = f"Unsupported batch encoding format: {encoding.format}"
                raise NotImplementedError(msg)

            # Delete Files Once injested.
            asyncio.run(self.cleanup_batch_files(file_path=file_path))

    def set_target_table(self, full_table_name: str) -> None:
        """Populates the property _target_table."""
        # We need to grab the schema_name and table_name
        # for the Table class instance
        _, schema_name, table_name = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)

        # You also need a blank MetaData instance
        # for the Table class instance
        meta = sa.MetaData()

        # This is the Table instance that will autoload
        # all the info about the table from the target server
        table: sa.Table = sa.Table(table_name, meta, autoload_with=self.connector._engine, schema=schema_name)

        self._target_table = table

    def set_insert_statement(self) -> None:
        """Populated the property _insert_statement."""
        insert_stmt = postgresql.insert(self.target_table)
        insert_stmt_skip = insert_stmt.on_conflict_do_nothing(
            constraint=self.target_table.primary_key
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint=self.target_table.primary_key
            ,set_=self.target_table.columns
        )
        if self.config["load_method"] == TargetLoadMethods.UPSERT:
            if self.target_table.primary_key:
                self._insert_statement = upsert_stmt
            else:
                self._insert_statement = insert_stmt
        else:
            self._insert_statement = insert_stmt

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> t.Optional[int]:
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

        if self._insert_statement is None:
            self.set_insert_statement()

        conformed_records = [self.conform_record(record) for record in records]

        # This is a insert based off SQLA example
        # https://docs.sqlalchemy.org/en/20/dialects/mssql.html#insert-behavior
        rowcount: int = 0
        try:
            with self.connector._connect() as conn, conn.begin():  # noqa: SLF001
                result:sa.CursorResult = conn.execute(
                    self._insert_statement,
                    conformed_records)
            rowcount = result.rowcount
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            self.logger.info(error)

        return rowcount
