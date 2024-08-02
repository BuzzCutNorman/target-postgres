"""postgres target class."""

from __future__ import annotations

import decimal
import sys
import typing as t

import msgspec
from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_postgres.sinks import PostgresSink

msg_buffer = bytearray(64)

def dec_hook(type: type, obj: t.Any) -> t.Any:  # noqa: ARG001, A002, ANN401
    """Decoding type helper for non native types.

    Args:
        type: the type given
        obj: the item to be decoded

    Returns:
        The object converted to the appropriate type, default is str.
    """
    return str(obj)


decoder = msgspec.json.Decoder(dec_hook=dec_hook, float_hook=decimal.Decimal)

class Targetpostgres(SQLTarget):
    """Sample target for postgres."""

    name = "target-postgres"
    default_sink_class = PostgresSink

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey",
            required=True,
            allowed_values=["postgresql"],
            default="postgresql"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server",  # noqa: E501
            required=True,
            allowed_values=["psycopg", "psycopg2", "pg8000", "asyncpg", "psycopg2cffi"],
            default="psycopg"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance",
            required=True
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server",  # noqa: E501
            required=True
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            required=True,
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection",
            required=True
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="The Default schema to place all streams"
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "executemany_mode",
                    th.StringType,
                    description="Executemany Mode: values_plus_batch,"
                ),
                th.Property(
                    "executemany_values_page_size",
                    th.IntegerType,
                    description="Executemany Values Page Size: Number:,"
                ),
                th.Property(
                    "executemany_batch_page_size",
                    th.IntegerType,
                    description="Executemany Batch Page Size: Number:,"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Paramaters: executemany_mode, future"
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression options is gzip",  # noqa: E501
                        )
                    )
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description=("the directory you want batch messages to be placed in\n"  # noqa: E501
                                        "example: file://test/batches"
                            )
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description=("What prefix you want your messages to have\n"
                                        "example: test-batch-"
                            )
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Turn on translation of Higher Defined(HD) JSON Schema types to SQL Types."  # noqa: E501
        ),
    ).to_dict()

    default_input = sys.stdin.buffer

    def deserialize_json(self, line: bytes) -> dict:  # noqa: PLR6301
        """Deserialize a line of json.

        Args:
            line: A single line of json.

        Returns:
            A dictionary of the deserialized json.

        Raises:
            msgspec.DecodeError: raised if any lines are not valid json
        """
        try:
            return decoder.decode(  # type: ignore[no-any-return]
                line,
            )
        except msgspec.DecodeError as exc:
            self.logger.exception("Unable to parse:\n%s", line, exc_info=exc)
            raise

if __name__ == "__main__":
    Targetpostgres.cli()
