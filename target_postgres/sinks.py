"""postgres target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Any, Dict, cast, Iterable, Optional

import sqlalchemy
from sqlalchemy import DDL, Table, MetaData, exc, types, engine_from_config
from sqlalchemy.dialects import postgresql
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
        eng_config = {f"{eng_prefix}url":self.sqlalchemy_url,f"{eng_prefix}echo":"False"}

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return engine_from_config(eng_config, prefix=eng_prefix)

    def create_schema(self, schema_name: str) -> None:
        """Create target schema.

        Args:
            schema_name: The target schema to create.
        """
        with self._engine.connect() as conn:
                conn.execute(sqlalchemy.schema.CreateSchema(schema_name))
                
    def to_sql_type(self, jsonschema_type: dict) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        if self.config.get('hd_jsonschema_types',False):
            return self.hd_to_sql_type(jsonschema_type)
        else: 
            return self.org_to_sql_type(jsonschema_type)

    @staticmethod
    def org_to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
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

    @staticmethod
    def hd_to_sql_type(jsonschema_type: dict) -> types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.
        
        Developers may optionally add custom logic before calling the default implementation
        inherited from the base class.
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
            length:int = jsonschema_type.get('maxLength')
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

        # JSON Numbers to Postgres 
        if 'number' in jsonschema_type.get('type'):  
            minimum = jsonschema_type.get('minimum')
            maximum = jsonschema_type.get('maximum')
            if (minimum == -922337203685477.6) and (maximum == 922337203685477.6):
            # There is something that is traucating and rounding this number
            # if (minimum == -922337203685477.5808) and (maximum == 922337203685477.5807):
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
                # A check for 'e+' in the string of the value is what I key off.
                # If it is no present we can count the number of '9' in the string.
                # If it is present we need to do a little more parsing to translate.
                if 'e+' not in str(maximum):
                    precision = str(maximum).count('9')
                    scale = precision - str(maximum).rfind('.')
                    return cast(sqlalchemy.types.TypeEngine, postgresql.NUMERIC(precision=precision,scale=scale))
                else:
                    precision_start = str(maximum).rfind('+')
                    precision = int(str(maximum)[precision_start:])
                    scale_start = str(maximum).find('.') + 1
                    scale_end = str(maximum).find('e')
                    scale = scale_end - scale_start
                    return cast(sqlalchemy.types.TypeEngine, postgresql.NUMERIC(precision=precision,scale=scale))  
                
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
        name = name.lower()
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
        statement = postgresql.insert(self.connector.get_table(full_table_name))

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

        conformed_records = (
            [self.conform_record(record) for record in records]
            if isinstance(records, list)
            else (self.conform_record(record) for record in records)
        )

        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )

        try:
            with self.connector.connection.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        insert_sql,
                        conformed_records,
                    )
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.