# Salesforce Pub/Sub Connector: Technical Methodology

This document explains the architecture and implementation of the Salesforce Pub/Sub connector, covering how it uses **Protocol Buffers (Protobuf)**, **gRPC**, **Avro**, and other technologies to stream Change Data Capture (CDC) events from Salesforce into Databricks Lakehouse.

---

## Table of Contents

1. [Overview](#overview)
2. [Salesforce Pub/Sub API Architecture](#salesforce-pubsub-api-architecture)
3. [Technology Stack](#technology-stack)
4. [Connector Architecture](#connector-architecture)
5. [Authentication Flow](#authentication-flow)
6. [Event Streaming via gRPC](#event-streaming-via-grpc)
7. [Protobuf Message Structure](#protobuf-message-structure)
8. [Avro Schema & Event Decoding](#avro-schema--event-decoding)
9. [CDC Bitmap Processing](#cdc-bitmap-processing)
10. [Offset Management & Checkpointing](#offset-management--checkpointing)
11. [Integration with Lakeflow Connect](#integration-with-lakeflow-connect)
12. [Deploying the Pipeline](#deploying-the-pipeline)
13. [Key Design Decisions](#key-design-decisions)

---

## Overview

The Salesforce Pub/Sub connector enables real-time streaming of Salesforce events into Databricks Lakehouse. The Salesforce Pub/Sub API provides a scalable, event-driven mechanism to capture changes:

| Event Type | Description | Topic Format |
|------------|-------------|--------------|
| **Change Data Capture (CDC)** | Real-time changes to Salesforce objects (creates, updates, deletes, undeletes) | `/data/{Object}ChangeEvent` |
| **Platform Events** | Custom event messages published by Salesforce applications | `/event/{EventName}__e` |
| **Custom Events** | User-defined event schemas for application-specific needs | `/event/{CustomEvent}__e` |

### Unified Change Events Topic

Salesforce provides a special topic `/data/ChangeEvents` that streams changes from **all CDC-enabled standard and custom objects** in a single subscription. This is useful for:

- **Centralized ingestion**: Capture all Salesforce changes through a single streaming table, then fan out to object-specific tables downstream
- **Simplified architecture**: Reduce the number of subscriptions and streaming jobs needed
- **Discovery**: Monitor all changes across the org without pre-configuring individual object topics

When using `/data/ChangeEvents`, the `ChangeEventHeader.entityName` field in each event identifies the source object (e.g., `Account`, `Contact`, `Lead`), allowing downstream processing to route events appropriately.

The connector is a **single-file, self-contained implementation** that bundles all required components—including protobuf definitions, gRPC stubs, and utility functions—to simplify deployment in distributed Spark environments.

---

## Salesforce Pub/Sub API Architecture

The data flows from Salesforce objects through an internal Event Bus to the Pub/Sub API, where clients can subscribe to receive events:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SALESFORCE ORG                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                       │
│  │   Account    │  │   Contact    │  │  Custom Obj  │   ← Salesforce Objects│
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                       │
│         │                 │                 │                               │
│         ▼                 ▼                 ▼                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    SALESFORCE EVENT BUS                              │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │   │
│  │  │ AccountChange   │  │ ContactChange   │  │ Platform Events │       │   │
│  │  │     Event       │  │     Event       │  │                 │       │   │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │   │
│  └───────────┼────────────────────┼────────────────────┼────────────────┘   │
└──────────────┼────────────────────┼────────────────────┼────────────────────┘
               │                    │                    │
               ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      SALESFORCE PUB/SUB API                                  │
│                   (api.pubsub.salesforce.com:7443)                           │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                         gRPC Service                                   │  │
│  │  • Subscribe() - Server-streaming RPC for event consumption            │  │
│  │  • GetTopic()  - Unary RPC for topic metadata                          │  │
│  │  • GetSchema() - Unary RPC for Avro schema retrieval                   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
               │
               │ gRPC over TLS (Protobuf messages)
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    LAKEFLOW CONNECTOR                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ PubSubAPI    │  │   Avro       │  │   Bitmap     │  │  Lakeflow    │      │
│  │   Client     │──│  Decoder     │──│  Processor   │──│  Connect     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘      │
└──────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAKEHOUSE                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Delta Lake Tables                                    │ │
│  │  • account_change_events                                                │ │
│  │  • contact_change_events                                                │ │
│  │  • platform_events                                                      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

The connector leverages multiple serialization and communication technologies, each serving a specific purpose in the streaming pipeline:

### gRPC (Google Remote Procedure Call)

gRPC provides the transport layer between the connector and Salesforce's Pub/Sub API:

- **Server Streaming**: The connector sends a subscription request and receives a continuous stream of events
- **TLS Encryption**: All communication is secured over port 7443
- **Efficient Binary Protocol**: Lower latency and bandwidth compared to REST APIs
- **Flow Control**: The client specifies how many events to request per batch, preventing overwhelming the consumer

### Protocol Buffers (Protobuf)

Protobuf defines the structure of messages exchanged with the Pub/Sub API:

- **Binary Wire Format**: Compact serialization for efficient network transmission
- **Strongly Typed Contracts**: Message schemas are defined in `.proto` files
- **Embedded Descriptors**: The connector includes serialized proto descriptors to avoid external dependencies

### Apache Avro

Avro is used for event payload serialization within the Pub/Sub events:

- **Schema Evolution**: Supports adding/removing fields without breaking consumers
- **Dynamic Schema Resolution**: Schema is retrieved from Salesforce via the `GetSchema` RPC and parsed at runtime
- **Compact Binary Format**: Efficient encoding for event payloads

### Bitstring Processing

Salesforce CDC events use bitmap fields to compactly represent which fields changed:

- **Hexadecimal Bitmaps**: Field changes encoded as hex strings (e.g., `0x0C01`)
- **Position Mapping**: Each bit position corresponds to a field index in the Avro schema
- **Nested Bitmap Support**: Compound fields use `parentIndex-childBitmap` format

---

## Connector Architecture

The connector is organized as a single Python file with layered components:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         salesforce_pubsub.py                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PROTOBUF DEFINITIONS                                                │   │
│  │  Serialized proto descriptors with lazy class initialization         │   │
│  │  Avoids Spark pickling issues by deferring protobuf class creation   │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  GRPC STUB (PubSubStub)                                              │   │
│  │  • Subscribe() - Stream events from a topic                          │   │
│  │  • GetSchema() - Fetch Avro schema by ID                             │   │
│  │  • GetTopic()  - Get topic metadata and schema ID                    │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  BITMAP PROCESSORS                                                   │   │
│  │  Convert hex bitmaps to human-readable field names                   │   │
│  │  Handles both top-level and nested compound field bitmaps            │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PUBSUB API CLIENT                                                   │   │
│  │  • OAuth 2.0 Client Credentials authentication                       │   │
│  │  • SOAP Password authentication (legacy)                             │   │
│  │  • Schema caching and event subscription management                  │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  LAKEFLOW CONNECT (Main Interface)                                   │   │
│  │  • list_tables()          - Return available event tables            │   │
│  │  • get_table_schema()     - Return Spark StructType schema           │   │
│  │  • read_table_metadata()  - Return primary keys and ingestion type   │   │
│  │  • read_table()           - Stream events with offset management     │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Authentication Flow

The connector supports two authentication methods. All authenticated sessions provide three key values needed for gRPC calls: `access_token`, `instance_url`, and `tenant_id`.

### OAuth 2.0 Client Credentials (Recommended)

This flow uses a Connected App's consumer key and secret:

```
┌──────────────┐                              ┌─────────────────────┐
│  Connector   │                              │     Salesforce      │
└──────┬───────┘                              └──────────┬──────────┘
       │                                                 │
       │  POST /services/oauth2/token                    │
       │  grant_type=client_credentials                  │
       │────────────────────────────────────────────────>│
       │                                                 │
       │  {access_token, instance_url}                   │
       │<────────────────────────────────────────────────│
       │                                                 │
       │  GET /services/oauth2/userinfo                  │
       │────────────────────────────────────────────────>│
       │                                                 │
       │  {organization_id}                              │
       │<────────────────────────────────────────────────│
```

### SOAP Password Authentication (Legacy)

Uses username and password + security token via SOAP API:

```
┌──────────────┐                              ┌─────────────────────┐
│  Connector   │                              │     Salesforce      │
└──────┬───────┘                              └──────────┬──────────┘
       │                                                 │
       │  POST /services/Soap/u/60.0                     │
       │  <login><username/><password/></login>          │
       │────────────────────────────────────────────────>│
       │                                                 │
       │  <loginResponse>                                │
       │    <sessionId/><serverUrl/><organizationId/>    │
       │  </loginResponse>                               │
       │<────────────────────────────────────────────────│
```

### gRPC Metadata

All gRPC calls include authentication metadata as headers:

| Header | Value |
|--------|-------|
| `accesstoken` | Session token from authentication |
| `instanceurl` | Salesforce instance URL (e.g., `https://na1.salesforce.com`) |
| `tenantid` | Organization/Tenant ID |

---

## Event Streaming via gRPC

The connector uses the `Subscribe` RPC to receive events from Salesforce. This is a server-streaming call where the client sends a `FetchRequest` and receives a stream of `FetchResponse` messages containing events.

### Subscription Flow

```
┌──────────────┐                              ┌─────────────────────┐
│  Connector   │                              │   Pub/Sub API       │
└──────┬───────┘                              └──────────┬──────────┘
       │                                                 │
       │  FetchRequest                                   │
       │  {topic, replay_preset, num_requested}          │
       │────────────────────────────────────────────────>│
       │                                                 │
       │                              FetchResponse      │
       │                              {events[], latest_replay_id}
       │<────────────────────────────────────────────────│
       │                                                 │
       │                              FetchResponse      │
       │                              {events[], latest_replay_id}
       │<────────────────────────────────────────────────│
       │                                                 │
       │  ... stream continues until timeout or limit ...│
```

### Flow Control

The `num_requested` field in `FetchRequest` controls how many events the server should send before waiting for another request. This prevents overwhelming the client when there's a large backlog of events.

### Replay Presets

| Preset | Behavior |
|--------|----------|
| `EARLIEST` | Start from the oldest retained event (up to 72 hours) |
| `LATEST` | Start from the newest event, skip historical backlog |
| `CUSTOM` | Resume from a specific `replay_id` (for checkpointing) |

---

## Protobuf Message Structure

The Pub/Sub API uses these key message types:

### FetchRequest (Client → Server)

| Field | Type | Description |
|-------|------|-------------|
| `topic_name` | string | Topic path (e.g., `/data/AccountChangeEvent`) |
| `replay_preset` | enum | EARLIEST, LATEST, or CUSTOM |
| `replay_id` | bytes | Position to resume from (when preset=CUSTOM) |
| `num_requested` | int32 | Max events to receive before flow control pause |

### FetchResponse (Server → Client)

| Field | Type | Description |
|-------|------|-------------|
| `events` | ConsumerEvent[] | Array of received events |
| `latest_replay_id` | bytes | Most recent position in the stream |
| `pending_num_requested` | int32 | Remaining flow control budget |

### ConsumerEvent

| Field | Type | Description |
|-------|------|-------------|
| `event.id` | string | Unique event identifier |
| `event.schema_id` | string | Reference to Avro schema for decoding |
| `event.payload` | bytes | Avro-encoded event data |
| `replay_id` | bytes | Stream position for this event |

### Embedded Protobuf Descriptors

Rather than generating code from `.proto` files, the connector embeds serialized proto descriptors and builds message classes at runtime. This approach:

- Eliminates dependency on generated protobuf code
- Enables single-file distribution
- Uses lazy initialization to avoid Spark serialization issues

---

## Avro Schema & Event Decoding

### Schema Retrieval

Each event references a `schema_id` that maps to an Avro schema. The connector fetches schemas via the `GetSchema` RPC and caches them for reuse:

1. Topic info provides the default schema ID
2. Schema JSON is fetched from Salesforce via gRPC
3. JSON is parsed into an Avro schema object
4. Schema is cached by ID for subsequent events

### Decoding Pipeline

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Binary Payload │───>│  Avro Decoder   │───>│  Python Dict    │
│  (from event)   │    │  (with schema)  │    │  (decoded data) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

The Avro `DatumReader` uses the fetched schema to decode the binary payload into a Python dictionary.

### Example Decoded CDC Event

```json
{
  "ChangeEventHeader": {
    "entityName": "Account",
    "recordIds": ["001xx000003DGXXAA4"],
    "changeType": "UPDATE",
    "changedFields": ["0x0C01"],
    "commitTimestamp": 1704067200000,
    "commitUser": "005xx000001SvMAA0"
  },
  "Name": "Updated Account Name",
  "Industry": "Technology",
  "Website": null
}
```

---

## CDC Bitmap Processing

Salesforce CDC events use **bitmap fields** to efficiently indicate which fields were modified. The connector decodes these into human-readable field names.

### Bitmap Encoding

The `changedFields`, `nulledFields`, and `diffFields` arrays contain hexadecimal bitmap strings:

```
Bitmap: "0x0C01"

Hex to Binary:
  0x0C = 00001100
  0x01 = 00000001
  Combined: 0000110000000001

Reverse (LSB first):
  1000000000110000

Find '1' positions:
  Position 0, 11, 12

Map to schema fields:
  field[11] = "Name"       → changed
  field[12] = "Industry"   → changed
```

### Nested Compound Fields

For compound fields (like `BillingAddress`), bitmaps use a `parentIndex-childBitmap` format:

```
"5-0x03" means:
  • Parent field at index 5 (e.g., BillingAddress)
  • Child bitmap 0x03 → bits 0,1 → Street, City
  
Result: ["BillingAddress.Street", "BillingAddress.City"]
```

### Enriched Output

The connector adds `*Names` fields with decoded field names:

```json
{
  "ChangeEventHeader": {
    "changedFields": ["0x0C01"],
    "changedFieldsNames": ["Name", "Industry"],
    "nulledFields": [],
    "nulledFieldsNames": [],
    "diffFields": [],
    "diffFieldsNames": []
  }
}
```

---

## Offset Management & Checkpointing

### Replay ID as Offset

Each event has a unique `replay_id` (binary bytes) representing its position in the stream. This enables exactly-once semantics through checkpointing:

```
Event Stream:
┌───────┬───────┬───────┬───────┬───────┬───────┐
│ E1    │ E2    │ E3    │ E4    │ E5    │ E6    │
│ ID:A1 │ ID:A2 │ ID:A3 │ ID:A4 │ ID:A5 │ ID:A6 │
└───────┴───────┴───────┴───────┴───────┴───────┘
              ↑                       ↑
       Last checkpoint         Latest available
```

### Storage Format

Replay IDs are stored as base64-encoded strings for serialization compatibility:

| Operation | Format |
|-----------|--------|
| Wire format | Binary bytes |
| Storage/checkpoint | Base64-encoded string |

### Resumption Logic

| Scenario | Replay Preset | Behavior |
|----------|---------------|----------|
| Initial load (no offset) | `EARLIEST` | Start from oldest available |
| Incremental (has offset) | `CUSTOM` | Resume from stored replay_id |
| Real-time only | `LATEST` | Skip backlog, newest only |

---

## Integration with Lakeflow Connect

The connector implements the standard `LakeflowConnect` interface:

### Interface Methods

| Method | Purpose |
|--------|---------|
| `__init__(options)` | Initialize with auth credentials and connection settings |
| `list_tables()` | Return table names derived from configured topics |
| `get_table_schema(table_name, table_options)` | Return Spark StructType for the event table |
| `read_table_metadata(table_name, table_options)` | Return primary keys, cursor field, ingestion type |
| `read_table(table_name, start_offset, table_options)` | Stream events with offset management |

### Output Schema

All Pub/Sub event tables share a common schema:

| Column | Type | Description |
|--------|------|-------------|
| `replay_id` | STRING | Unique event identifier (base64) |
| `event_payload` | BINARY | Raw Avro-encoded payload |
| `schema_id` | STRING | Avro schema reference |
| `topic_name` | STRING | Source topic path |
| `timestamp` | LONG | Event receipt time (milliseconds) |
| `decoded_event` | STRING | JSON-decoded payload with bitmap processing |

### Metadata

| Field | Value |
|-------|-------|
| `primary_keys` | `["replay_id"]` |
| `cursor_field` | `"replay_id"` |
| `ingestion_type` | `"append"` (events are immutable) |

### Topic-to-Table Naming

Topics are converted to snake_case table names:

| Topic | Table Name |
|-------|------------|
| `/data/AccountChangeEvent` | `account_change_event` |
| `/data/ContactChangeEvent` | `contact_change_event` |
| `/event/MyPlatformEvent__e` | `my_platform_event` |

---

## Deploying the Pipeline

This section walks through deploying the Salesforce Pub/Sub connector as a Lakeflow Community Connector pipeline on Databricks.

### Prerequisites

The community connector requires specific Python packages to be installed in the pipeline environment. In your Databricks pipeline settings, navigate to the **Environment** tab under serverless compute and add these packages as dependencies:

```
grpcio>=1.50.0
protobuf>=4.21.0
avro-python3>=1.10.0
bitstring>=4.0.0
certifi>=2022.0.0
requests>=2.28.0
```

### Step 1: Create a Unity Catalog Connection

Create a Community Connector connection in Unity Catalog for Salesforce with your chosen authentication mechanism:

**For OAuth 2.0 Client Credentials:**
- `clientId`: Your Connected App Consumer Key
- `clientSecret`: Your Connected App Consumer Secret
- `loginUrl`: Salesforce login URL (e.g., `https://login.salesforce.com` for production or `https://test.salesforce.com` for sandbox)

**For Password Authentication (Legacy):**
- `username`: Your Salesforce username
- `password`: Your Salesforce password + security token
- `loginUrl`: Salesforce login URL

### Step 2: Deploy the Connector

Deploy the connector from this repository using the Lakeflow Community Connector CLI:

```bash
# Use salesforce_pubsub as the source name
databricks labs community-connector deploy --source salesforce_pubsub
```

### Step 3: Create the Ingestion Pipeline

Create an ingestion pipeline definition file (e.g., `ingest.py`) that defines streaming tables for each Salesforce topic you want to capture:

```python
import dlt
from dlt import read_stream

# Stream Account changes
@dlt.table(name="account_change_event")
def account_changes():
    return read_stream(
        "lakeflow_connect",
        connection="salesforce_pubsub_connection",
        table_options={"topic": "/data/AccountChangeEvent"}
    )

# Stream Contact changes
@dlt.table(name="contact_change_event")
def contact_changes():
    return read_stream(
        "lakeflow_connect",
        connection="salesforce_pubsub_connection",
        table_options={"topic": "/data/ContactChangeEvent"}
    )

# Stream Lead changes
@dlt.table(name="lead_change_event")
def lead_changes():
    return read_stream(
        "lakeflow_connect",
        connection="salesforce_pubsub_connection",
        table_options={"topic": "/data/LeadChangeEvent"}
    )

# Stream Opportunity changes
@dlt.table(name="opportunity_change_event")
def opportunity_changes():
    return read_stream(
        "lakeflow_connect",
        connection="salesforce_pubsub_connection",
        table_options={"topic": "/data/OpportunityChangeEvent"}
    )
```

### Alternative: Unified Change Events Ingestion

For a centralized approach, use the `/data/ChangeEvents` topic to capture all changes in a single table, then fan out downstream:

```python
import dlt
from dlt import read_stream

# Stream ALL Salesforce CDC changes into a single table
@dlt.table(name="all_salesforce_changes")
def all_changes():
    return read_stream(
        "lakeflow_connect",
        connection="salesforce_pubsub_connection",
        table_options={"topic": "/data/ChangeEvents"}
    )

# Fan out to object-specific tables using downstream views or tables
@dlt.table(name="account_changes_derived")
def account_changes_derived():
    return dlt.read("all_salesforce_changes").filter(
        "get_json_object(decoded_event, '$.ChangeEventHeader.entityName') = 'Account'"
    )
```

### Step 4: Configure and Run the Pipeline

1. Create a new Delta Live Tables pipeline in Databricks
2. Point it to your `ingest.py` file
3. Ensure the Python dependencies are configured in the Environment tab
4. Set the pipeline to **Continuous** mode for real-time streaming
5. Start the pipeline

Events will begin flowing from Salesforce to your Delta tables with sub-5-second latency.

---

## Key Design Decisions

### Single-File Architecture

**Problem**: Distributed Spark environments make dependency management complex.

**Solution**: Bundle all components—protobuf descriptors, gRPC stubs, utilities—into a single Python file. This enables simple deployment via the `merge_python_source.py` script.

### Lazy Protobuf Initialization

**Problem**: Protobuf descriptor pools aren't pickle-safe, causing Spark serialization errors when registering DataSources.

**Solution**: Store serialized proto bytes as a constant and build message classes on-demand via `_get_protobuf_classes()`. Classes are only created when gRPC calls are made, not at import time.

### Serializable Client State

**Problem**: gRPC channels and stubs can't be pickled for distribution across Spark executors.

**Solution**: Custom `__getstate__`/`__setstate__` methods exclude gRPC objects from serialization. Connections are lazily re-established on each executor.

### Timeout-Based Batching

**Problem**: Balance between latency (process events quickly) and throughput (batch for efficiency).

**Solution**: Collect events for a configurable `pollTimeoutSeconds` or until `maxEventsPerBatch` is reached, whichever comes first. This creates natural micro-batches suitable for streaming workloads.

### Schema Caching

**Problem**: Fetching schemas for every event would be inefficient.

**Solution**: Cache Avro schemas by `schema_id`. Most events from the same topic share a schema, so caching dramatically reduces API calls.

---

## Summary

The Salesforce Pub/Sub connector combines multiple technologies for reliable, real-time CDC streaming:

| Layer | Technology | Purpose |
|-------|------------|---------|
| Transport | **gRPC** | Efficient streaming over TLS |
| Wire Format | **Protobuf** | Structured message serialization |
| Payload | **Avro** | Schema-aware event encoding |
| Field Tracking | **Bitmap Processing** | Decode changed field indicators |
| Interface | **LakeflowConnect** | Standardized connector API |

This architecture enables scalable ingestion of Salesforce events into the Databricks Lakehouse with exactly-once semantics and minimal operational overhead.
