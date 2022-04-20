# TODO: Figure out if actions can be registered as part of the datahub CLI. 

# DataHub Actions Framework

Welcome to DataHub Actions! This framework provides a mechanism to write custom code which can be executed when events happen on DataHub. 

## Installation

To install the Actions framework, simply install the "datahub-actions" package from Acryl. 

```
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions
datahub-actions --version
```

## Actions Library

By default, DataHub supports a set of standard actions plugins. These include the following:

TODO: Insert Table Here. 

## Configuring an Action

Actions are configured using a YAML file, much in the same way DataHub ingestion sources are. An action configuration file consists of the following:

- `version`: The version of the action config file. Defaults to 0.
- `actions`: A list of actions to configure.
- `actions.type`: The type of the action to configure. This can be a common name or a fully-qualified class name.
- `actions.config`: Action-specific configurations. 
- `server`: The location where DataHub GMS is deployed. E.g.
    - `http://localhost:9002/api/gms`
    - `http://localhost:8080`
    - `https://datahub-your-org.com/api/gms`
- `token`: An access token to use for accessing DataHub APIs. 


### Sample Configuration

```
TODO: 
```

Once you've configured these properties, you can continue on to the next step. 

## Running an Action

To run a new action, just use the datahub-actions CLI to start an actions listener. 

```
datahub-actions run -c actions-config.yml 
```

## FAQ

## Default Actions

By default, DataHub ships with an important action called the "Executor" action. This is used for running ingestion which is scheduled through the DataHub User Interface. This action can be disabled
by running: 

but we encourage you to keep it around. 
