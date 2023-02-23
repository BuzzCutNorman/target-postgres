# target-postgres

`target-postgres` is a Singer target for PostgreSQL. !!! Warning !!! really early version.  It works barely 😵 for full table loads. 

Build with the [Meltano Target SDK](https://sdk.meltano.com).
### Whats New 🛳️🎉
**2023-02-08 Higher Defined(HD) JSON Schema types:**  Translates the Higher Defined(HD) JSON Schema types from the buzzcutnorman `tap-mssql` back into PostgreSQL data types.  You can give it a try by setting `hd_jsonschema_types` to `True` in your config.json or meltano.yml

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
| Setting              | Required | Default | Description |
|:---------------------|:--------:|:-------:|:------------|
| dialect              | False    | None    | The Dialect of SQLAlchamey |
| driver_type          | False    | None    | The Python Driver you will be using to connect to the SQL server |
| host                 | False    | None    | The FQDN of the Host serving out the SQL Instance |
| port                 | False    | None    | The port on which SQL awaiting connection |
| user                 | False    | None    | The User Account who has been granted access to the SQL Server |
| password             | False    | None    | The Password for the User account |
| database             | False    | None    | The Default database for this connection |
| default_target_schema| False    | None    | The Default schema to place all streams |
| sqlalchemy_eng_params| False    | None    | SQLAlchemy Engine Paramaters: executemany_mode, future |
| batch_config         | False    | None    | Optional Batch Message configuration |
| hd_jsonschema_types  | False   |       0 | Turn on translation of Higher Defined(HD) JSON Schema types to SQL Types |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
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
