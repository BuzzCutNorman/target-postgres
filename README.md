# target-postgres

`target-postgres` is a Singer target for PostgreSQL. !!! Warning !!! really early version.  It works ok 😐. 

Build with the [Meltano Target SDK](https://sdk.meltano.com).
### Whats New 🛳️🎉
**2024-07-12 Upgraded to Meltano Singer-SDK 0.36.1**

**2024-01-31 Upgraded to Meltano Singer-SDK 0.34.1:** Happy New Year!!!🎉.  My goal was to start using tags and releases by 2024 and was pretty close.  You can now lock on a release number if you want. 

**2023-10-16 Upgraded to Meltano Singer-SDK 0.32.0:** SQLAlchemy 2.x is main stream in this version so I took advantage of that and bumped from `1.4.x` to `2.x`.  SQLAlchemy supports `psycopg` so I added it as a dependency and you can now use it as a driver option. In the `hd_jsonschema_types` the `minimum` and `maximum` values used to define `NUMERIC` or `DECIMAL` precision and scale values were being rounded.  This caused an issue with the translation on the target side.  I leveraged scientific notation to resolve this.

**2023-04-26 New HD JSON Schema Types:**  Added translations for HD JSON Schema definitions of Xml and Binary types from the buzzcutnorman `tap-mssql`.  This is Thanks🙏 to Singer-SDK 0.24.0 which allows for JSON Schema `contentMediaType` and `contentEncoding`.  Currently all Binary data types are decoded before being inserted as BYTEA.  XML types are not supported in SQLAlchemy for PostgreSQL so are they are inserted as TEXT.

**2023-02-23 Higher Defined(HD) JSON Schema types:**  Translates the Higher Defined(HD) JSON Schema types from the buzzcutnorman `tap-mssql` back into PostgreSQL data types.  You can give it a try by setting `hd_jsonschema_types` to `True` in your config.json or meltano.yml

## Installation
Install from GitHub:

```bash
pipx install git+https://github.com/BuzzCutNorman/target-postgres.git
```

Install using [Meltano](https://www.meltano.com) as a [Custom Plugin](https://docs.meltano.com/guide/plugin-management#custom-plugins)


## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-postgres --about --format=markdown
```
-->
| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| dialect | True     | postgresql | The Dialect of SQLAlchamey |
| driver_type | True     | psycopg | The Python Driver you<BR/>will be using to connect<BR/>to the SQL server |
| host | True     | None    | The FQDN of the Host serving<BR/>out the SQL Instance |
| port | False    | None    | The port on which SQL<BR/>awaiting connection |
| user | True     | None    | The User Account who has<BR/>been granted access to the<BR/>SQL Server |
| password | True     | None    | The Password for the<BR/>User account |
| database | True     | None    | The Default database for<BR/>this connection |
| default_target_schema | False    | None    | The Default schema to<BR/>place all streams |
| sqlalchemy_eng_params | False    | None    | SQLAlchemy Engine Paramaters:<BR/>executemany_mode, future |
| sqlalchemy_eng_params.executemany_mode | False    | None    | Executemany Mode:<BR/>values_plus_batch, |
| sqlalchemy_eng_params.executemany_values_page_size | False    | None    | Executemany Values Page Size:<BR/>Number:, |
| sqlalchemy_eng_params.executemany_batch_page_size | False    | None    | Executemany Batch Page Size:<BR/>Number:, |
| sqlalchemy_eng_params.future | False    | None    | Run the engine in 2.0 mode:<BR/>True, False |
| batch_config | False    | None    | Optional Batch Message<BR/>configuration |
| batch_config.encoding | False    | None    |             |
| batch_config.encoding.format | False    | None    | Currently the only format<BR/>is jsonl |
| batch_config.encoding.compression | False    | None    | Currently the only<BR/>compression option<BR/>is gzip |
| batch_config.storage | False    | None    |             |
| batch_config.storage.root | False    | None    | The directory you want<BR/>batch messages to be placed in<BR/>example: file://test/batches |
| batch_config.storage.prefix | False    | None    | What prefix you want your<BR/>messages to have<BR/>example: test-batch- |
| hd_jsonschema_types | False    |       False | Turn on translation of<BR/>Higher Defined(HD) JSON<BR/>Schema types to SQL Types |
| hard_delete | False    |       False | Hard delete records. |
| add_record_metadata | False    | None    | Add metadata to records. |
| load_method | False    | append-only | The method to use when<BR/>loading data into the <BR/>destination. `append-only`<BR/>will always write all input<BR/>records whether that records<BR/>already exists or not.<BR/>`upsert` will update existing<BR/>records and insert new records.<BR/>`overwrite` will delete all<BR/>existing records and insert<BR/>all input records. |
| batch_size_rows | False    | None    | Maximum number of rows in each batch. |
| validate_records | False    |       True | Whether to validate the<BR/>schema of the incoming streams. |
| stream_maps | False    | None    | Config object for stream<BR/>maps capability. For more<BR/>information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values<BR/>to be used within<BR/>map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/)<BR/>instance variable `fake`<BR/>used within map expressions.<BR/>Only applicable if the<BR/>plugin specifies `faker` as<BR/>an addtional dependency<BR/>(through the `singer-sdk`<BR/>`faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker<BR/>generator for<BR/>deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale<BR/>strings to produce localized<BR/>output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema<BR/>flattening and automatically<BR/>expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-postgres --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-postgres` by itself or in a pipeline using [Meltano](https://meltano.com/).
<!--
### Executing the Target Directly

```bash
target-postgres --version
target-postgres --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-postgres --config /path/to/target-postgres-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_postgres/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-postgres` CLI interface directly using `poetry run`:

```bash
poetry run target-postgres --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->
<!--
Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-postgres
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-postgres --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-postgres
```
-->
### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
