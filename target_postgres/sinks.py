"""postgres target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Any, Dict, cast, Iterable, Optional

import sqlalchemy
from sqlalchemy import DDL, Table, MetaData, exc, types, engine_from_config
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL,Engine

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

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for postgres.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Generates a SQLAlchemy URL for postgresql.

        Args:
            config: The configuration for the connector.
        """
        if config['dialect'] == "postgresql":
            url_drivername:str = config['dialect']
        else:
            cls.logger.error("Invalid dialect given")
            exit(1)

        if config['driver_type'] in ["psycopg2","pg8000","asyncpg","psycopg2cffi","pypostgresql","pygresql"]:
            url_drivername += f"+{config['driver_type']}"
        else:
            cls.logger.error("Invalid driver_type given")
            exit(1)

        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host = config['host'],
            database = config['database']
        )

        if 'port' in config:
            config_url.set(port=config['port'])
        
        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])
        
        return (config_url)

    def create_sqlalchemy_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {f"{eng_prefix}url":self.sqlalchemy_url,f"{eng_prefix}echo":"False"}

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return engine_from_config(eng_config, prefix=eng_prefix)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.
        
        Developers may optionally add custom logic before calling the default implementation
        inherited from the base class.
        """
        # Optionally, add custom logic before calling the super().
        # You may delete this method if overrides are not needed.
        #logger = logging.getLogger("sqlconnector")
        if jsonschema_type.get('format') == 'date-time':
            return cast(types.TypeEngine, sqlalchemy.types.TIMESTAMP())

        return SQLConnector.to_sql_type(jsonschema_type)


class postgresSink(SQLSink):
    """postgres target sink class."""

    connector_class = postgresConnector

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
        # # replace leading digit
        # return replace_leading_digit(name)
        return(name)
        #return super().conform_name(name)

    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> str:
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        statement = insert(self.connector.get_table(full_table_name))

        return statement

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
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )

        try:
            with self.connector.connection.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        insert_sql,
                        records,
                    )
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.