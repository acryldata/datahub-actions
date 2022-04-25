# TODO: Figure out if actions can be registered as part of the datahub CLI. 

# DataHub Actions Framework

Welcome to DataHub Actions! This framework provides a mechanism to write custom code which can be executed when events happen on DataHub. 

## Installation

To install the Actions framework, simply install the "datahub-actions" package from Acryl. 

```
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions
datahub actions --version
```

## Actions Library

By default, DataHub supports a set of standard actions plugins. These include the following:

TODO: Insert Table Here. 

## Configuring an Action

Actions are configured using a YAML file, much in the same way DataHub ingestion sources are. An action configuration file consists of the following:

TODO


### Sample Configuration

```
TODO: 
```

Once you've configured these properties, you can continue on to the next step. 

## Running an Action

To run a new action, just use the datahub-actions CLI to start an actions listener. 

```
datahub actions -c <pipeline-config.yml>
```

### Running multiple actions

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

### Examples

#### Hello World Action

To run a "Hello World" action, which simply prints events it receives to the console,
you can run

```
datahub actions -c examples/hello_world.yaml
```

## FAQ

## Default Actions

By default, DataHub ships with an important action called the "Executor" action. This is used for running ingestion which is scheduled through the DataHub User Interface. This action can be disabled
by running: 

but we encourage you to keep it around. 
