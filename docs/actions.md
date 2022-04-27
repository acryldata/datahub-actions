# DataHub Actions

**DataHub Actions** is a python-based framework that makes responding to changes to your Metadata Graph in
realtime easy, enabling you to seamlessly integrate DataHub into a broader events-based architecture.

The framework consists of the following components & capabilities: 

- **Building Blocks**: Pluggable components for filering, transforming, and reacting to DataHub Events (e.g. tag is added, glossary term is removed, schema field is added, and so on.) in real time.
- **Actions Library**: An open library of freely available Transformers, Actions, Events, and more.
- **Configurable Semantics** 
    - **At-least Once Delivery**: Native support for independent processing state across individual Actions (via per-Action consumer group),
      and across executions of the same Action (via offsets), post-processing acking to achieve at-least once semantics for each Action.
    - **Robust Error Handling**: Configurable failure policies featuring event-retry, dead letter queue, and failed-event continuation policy
      to achieve the guarantees required by your organization.
  

### Use Cases

Real-time use cases broadly fall into the following categories:

- **Notification**: Generate organization-specific notifications when a change is made on DataHub. For example, send an email to the governance team when a "PII" tag is added to any data asset.
- **Automation**: Integrate DataHub into your organization's internal workflows. For example, create a Jira ticket when specific Tags or Terms are proposed on a Dataset.
- **Synchronization**: Syncing changes made in DataHub into a 3rd party system. For example, reflecting Tag additions in DataHub into Snowflake.
- **Auditing**: Audit who is making what changes on DataHub through time. 

and more!

### Getting Started

To get started immediately, check out the [DataHub Actions Quickstart](./quickstart.md) guides. 

## Concepts

The Actions Framework consists of a few core concepts:

- **Pipelines**
- **Events** and **Event Sources**
- **Transformers**
- **Actions**

Each of these will be described in detail below.

![](./imgs/actions.png)
*In the Actions Framework, Events flow continuously from left-to-right.** 

### Pipelines

A **Pipeline** is a continuously running process which performs the following functions:

1. Polls events from an configured Event Source (described below)
2. Applies configured Transformation + Filtering to the Event 
3. Excutes the configured Action on the resulting Event

in addition to handling initialization, errors, retries, logging, and more. 

Each Action Configuration file corresponds to a unique Pipeline. In practice,
each Pipeline has its very own Event Source, Transforms, and Actions. This makes it easy to maintain state for mission-critical Actions independently. 

Importantly, each Action must have a unique name. This serves as a stable identifier across Pipeline run which can be useful in saving the Pipeline's consumer state (ie. resiliency + reliability). For example, the Kafka Event Source (default) uses the pipeline name as the Kafka Consumer Group id. This enables you to easily scale-out your Actions by running multiple processes with the same exact configuration file. Each will simply become different consumers in the same consumer group, sharing traffic of the DataHub Events stream.

### Events

**Events** are data objects representing changes that have occurred on DataHub. Strictly speaking, the only requirement that the Actions framework imposes is that these objects must be 

a. Convertable to JSON
b. Convertable from JSON

So that in the event of processing failures, events can be written and read from a failed events file. 


#### Event Types

Each Event instance inside the framework corresponds to a single **Event Type**, which is common name (e.g. "EntityChangeEvent_v1") which can be used to understand the shape of the Event. This can be thought of as a "topic" or "stream" name. That being said, Events associated with a single type are not expected to change in backwards-breaking ways across versons.

Officially supported Event Types include:

1. `EntityChangeEvent_v1`: An Event that contains a higher level semantic change following a common base structure. For more details, check out [EntityChangeEvent](TODO). 

2. `MetadataChangeLogEvent_v1`: An Event that contains every change that has occurred on the Metadata Graph via a low-level event format. It is generally not recommended to depend strongly on this event, as it is subject to change when the DataHub internal Data mode changes. For more details, check out [MetadataChangeLogEvent]() 


### Event Sources

Events are produced to the framework by **Event Sources**. Event Sources may include their own guarantees, configurations, behaviors, and semantics. They usually produce a fixed set of Event Types. 

In addition to sourcing events, Event Sources are also responsible for acking the succesful processing of an event by implementing the `ack` method. This is invoked by the framework once the Event is guaranteed to have reached the configured Action successfully. 

The default Event Source is a Kafka Event Source. It is responsible for converting Kafka Events into **Events** that are understandable to the framework and then serving them to the coordinating **Pipeline**. More details about the Kafka Event Source can be found [here](TODO). 


### Transformers

**Transformers** are pluggable components which take an Event as input, and produce an Event (or nothing) as output. This can be used to enrich the information of an Event prior to sending it to an Action. 

Multiple Transformers can be configured to run in sequence, filtering and transforming an event in multiple steps.

Transformers can also be used to generate a completely new type of Event (i.e. registered at runtime via the Event Registry) which can subsequently serve as input to an Action. 

Transformers can be easily customized and plugged in to meet an organization's unqique requirements. For more information on developing a Transformer, check out [Developing a Transformer]('./guides/developing-a-transformer.md)


### Action

**Actions** are pluggable components which take an Event as input and perform some business logic. Examples may be sending a Slack notification, logging to a file,
or creating a Jira ticket, etc. 

Each Pipeline can be configured to have a single Action which runs after the filtering and transformations have occurred. 

Actions can be easily customized and plugged in to meet an organization's unqique requirements. For more information on developing a Action, check out [Developing a Action]('./guides/developing-an-action.md)


## Configuring an Actions Pipeline

Actions configuration files mirror the concepts discussed in the previous section.

Each configuration file maps to a single Actions **Pipeline** consists of a few parts:

1. **Name** (Required): The name of the Action Pipeline. This should be stable over time, as it can be used for various purposes
   such as defining a Kafka consumer group for the action that persists across pipeline runs. 
2. **Event Source Configuration** (Required - Defaults to Kafka): Configuration regarding the origin of events.
3. **Action Configuration** (Required): Configuration for the Action logic itself.
4. **Filter Configuration** (Optional): Configuration for filtering the stream of Events that reach the Action. (All events are forwarded by default)
5. **Transformer Configuration** (Optional): Configuration for transformations that should be applied to the stream of events post filtering.
6. **Pipeline Options** (Optional): Specific configurations around error handling, logging, and more at the Pipeline level.

We will describe each piece in detail below. 

