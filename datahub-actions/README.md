# ⚡ DataHub Actions Framework

Welcome to DataHub Actions! The Actions framework makes responding to changes to your Metadata Graph in
realtime easy, enabling you to seamlessly integrate DataHub into a broader events-based architecture.

For a detailed introduction, check out the [original announcement](https://www.youtube.com/watch?v=7iwNxHgqxtg&t=2189s) of the DataHub Actions Framework at the DataHub April 2022 Town Hall. For a more in-depth look at use cases and concepts, check out [DataHub Actions Concepts](../docs/concepts.md). 

## Quickstart

To get started right away, check out the [DataHub Actions Quickstart](../docs/quickstart.md) Guide.


## Prerequisites

The DataHub Actions CLI commands are an extension of the base `datahub` CLI commands. We recommend
first installing the `datahub` CLI:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub --version
```


## Installation

Next, simply install the `acryl-datahub-actions` package from PyPi:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions
datahub actions --version
```


## Configuring an Action

Actions are configured using a YAML file, much in the same way DataHub ingestion sources are. An action configuration file consists of the following

1. Action Pipeline Name (Should be unique and static)
2. Source Configurations
3. Transform + Filter Configurations
4. Action Configuration

With each component being independently pluggable and configurable. 

```yml
# 1. Required: Action Pipeline Name
name: <action-pipeline-name>

# 2. Required: Event Source - Where to source event from.
source:
  type: <source-type>
  config:
    # Event Source specific configs (map)

# 3a. Optional: Filter to run on events (map)
filter: 
  event_type: <filtered-event-type>
  event:
    # Filter event fields by exact-match
    <filtered-event-fields>

# 3b. Optional: Custom Transformers to run on events (array)
transform:
  - type: <transformer-type>
    config: 
      # Transformer-specific configs (map)

# 4. Required: Action - What action to take on events. 
action:
  type: <action-type>
  config:
    # Action-specific configs (map)
```

### Example: Hello World

An simple configuration file for a "Hello World" action, which simply prints all events it receives, is

```yml
# 1. Action Pipeline Name
name: "hello_world"
# 2. Event Source: Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
# 3. Action: What action to take on events. 
action:
  type: "hello_world"
```

We can modify this configuration further to filter for specific events, by adding a "filter" block.

```yml
# 1. Action Pipeline Name
name: "hello_world"

# 2. Event Source - Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}

# 3. Filter - Filter events that reach the Action
filter:
  event_type: "EntityChangeEvent_v1"
  event:
    category: "TAG"
    operation: "ADD"
    modifier: "urn:li:tag:pii"

# 4. Action - What action to take on events. 
action:
  type: "hello_world"
```


## Running an Action

To run a new action, just use the datahub-actions CLI to start an actions listener. 

```
datahub actions -c <config.yml>
```

If successful, you'll see a message like the following in the CLI output:

```
Actions Pipeline with name '<name>' is now running.
```

### Running multiple Actions

You can run multiple actions pipeline within the same command. Simply provide multiple 
config files by restating the "-c" command line argument.

For example,

```
datahub actions -c <config-1.yaml> -c <config-2.yaml>
```

### Running in debug mode

Simply append the `--debug` flag to the CLI to run your action in debug mode.

```
datahub actions -c <config.yaml> --debug
```


## Supported Events

Two event types are currently supported. Read more about them below.

- [Entity Change Event V1](../docs/events/entity-change-event.md)
- [Metadata Change Log V1](../docs/events/metadata-change-log-event.md)


## Supported Event Sources

Currently, the only event source that is officially supported is `kafka`, which polls for events
via a Kafka Consumer. 

- [Kafka Event Source](../docs/sources/kafka-event-source.md)


## Supported Actions

By default, DataHub supports a set of standard actions plugins. These can be found inside the folder
`src/datahub_actions/plugins`. 

Some pre-included Actions include

- [Hello World](../docs/actions/hello_world.md)
- [Executor](../docs/actions/executor.md)


## Development

### Build and Test

Notice that we support all actions command using a separate `datahub-actions` CLI entry point. Feel free 
to use this during development.

```
# Build datahub-actions module
./gradlew datahub-actions:build

# Drop into virtual env
cd datahub-actions && source venv/bin/activate 

# Start hello world action 
datahub-actions actions -c ../examples/hello_world.yaml

# Start ingestion executor action
datahub-actions actions -c ../examples/executor.yaml

# Start multiple actions 
datahub-actions actions -c ../examples/executor.yaml -c ../examples/hello_world.yaml
```

#### Developing a Transformer

To develop a new Transformer, check out the [Developing a Transformer](../docs/guides/developing-a-transformer.md) guide. 

#### Developing an Action

To develop a new Action, check out the [Developing an Action](../docs/guides/developing-an-action.md) guide. 


## Contributing

Contributing guidelines follow those of the [main DataHub project](https://github.com/datahub-project/datahub/blob/master/docs/CONTRIBUTING.md). We are accepting contributions for Actions, Transformers, and general framework improvements (tests, error handling, etc).


## Resources

Check out the [original announcement](https://www.youtube.com/watch?v=7iwNxHgqxtg&t=2189s) of the DataHub Actions Framework at the DataHub April 2022 Town Hall. 