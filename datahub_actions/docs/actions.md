# DataHub Actions

**DataHub Actions** is a python-based framework that makes responding to changes to your Metadata Graph in
realtime easy, enabling you to seamlessly integrate DataHub into a broader events-based architecture.

The framework consists of the following components & capabilities: 

- **Actions**: Pluggable components for running custom logic in response to DataHub Events (e.g. tag is added, glossary term is removed, schema field is added, and so on.)
- **Transforms** Pluggable components for filtering & transforming events when they are received
- **Events**: Pluggable components for defining custom Event types
- **Actions Library**: An open library of freely available Transformers, Actions, Events, and more.
- **Configurable Semantics** 
    - **At-least Once Delivery**: Native support for independent processing state across individual Actions (via per-Action consumer group),
      and across executions of the same Action (via offsets), post-processing acking to achieve at-least once semantics for each Action.
    - **Robust Error Handling**: Configurable failure policies featuring event-retry, dead letter queue, and failed-event continuation policy
      to achieve the guarantees required by your organization.
  

### Use Cases

Real-time use cases broadly fall into the following categories:

- **Notification**: Generate organization-specific notifications when a change is made on DataHub. For example, send an email to the governance team when a "PII" tag is added to any data asset.
- **Workflow Integration**: Integrate DataHub flows into your organization's internal workflow management system. For example, create a Jira ticket when specific Tags or Terms are proposed on a Dataset.
- **Synchronization**: Syncing changes made in DataHub into a 3rd party system. For example, reflecting Tag additions in DataHub into Snowflake.
- **Auditing**: Audit who is making what changes on DataHub through time. 

and more!

The remainder of this doc will help you get started with the DataHub Actions Framework.

### Getting Started

To get started immediately, check out the [DataHub Actions Quickstart](./quickstart.md) Guide. 

## Concepts

At a glance, the Actions Framework consists of a few core concepts, each of which will be described below.

![](./imgs/actions.png)

### Event Sources

### Filters

### Transformers

### Action


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

