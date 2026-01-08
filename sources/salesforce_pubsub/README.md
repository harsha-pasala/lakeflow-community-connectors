# Salesforce Pub/Sub Lakeflow Connector

This is a **self-contained** connector that enables streaming ingestion of Salesforce events via the Pub/Sub API, including:

- **Change Data Capture (CDC)** events from Salesforce objects
- **Platform Events** published to Salesforce
- **Custom Events** with user-defined schemas

## Package Structure

This connector is a **single-file implementation** designed to work with the
`merge_python_source.py` script for deployment to Spark Declarative Pipelines.

```
salesforce_pubsub/
├── __init__.py              # Package exports
├── salesforce_pubsub.py     # Complete self-contained connector
│                            # (includes protobuf, gRPC client, utilities)
├── configs/                 # Configuration files
│   ├── dev_config.json
│   └── dev_table_config.json
├── test/                    # Unit tests
│   └── test_salesforce_pubsub_lakeflow_connect.py
├── requirements.txt         # Python dependencies
└── README.md
```

The `salesforce_pubsub.py` file contains all components inlined:
- Protobuf message definitions for the Pub/Sub API
- gRPC stub for API communication
- `PubSubAPIClient` class for authentication and subscriptions
- Bitmap utility functions for CDC event decoding
- `LakeflowConnect` class implementing the connector interface

## Prerequisites

- Salesforce org with Pub/Sub API enabled
- Connected App with appropriate permissions (for OAuth) or user credentials

## Dependencies

Install required packages:

```bash
pip install grpcio protobuf avro-python3 bitstring certifi requests
```

Or from the requirements file:

```bash
pip install -r requirements.txt
```

## Authentication

The connector supports two authentication methods:

### OAuth Client Credentials (Recommended)

```python
# UC Connection - stores authentication only
connection_options = {
    "clientId": "your-connected-app-consumer-key",
    "clientSecret": "your-connected-app-consumer-secret",
    "loginUrl": "https://login.salesforce.com",  # Optional
}
```

### Password Authentication (Legacy)

```python
connection_options = {
    "username": "your-username@example.com",
    "password": "your-password-and-security-token",
    "loginUrl": "https://login.salesforce.com",  # Optional
}
```

## Configuration Options

### Connection Options (stored in UC Connection)

| Option | Description | Default |
|--------|-------------|---------|
| `clientId` | Connected App Consumer Key | - |
| `clientSecret` | Connected App Consumer Secret | - |
| `username` | Salesforce username | - |
| `password` | Salesforce password + security token | - |
| `loginUrl` | Salesforce login URL | `https://login.salesforce.com` |
| `grpcHost` | Pub/Sub API gRPC host | `api.pubsub.salesforce.com` |
| `grpcPort` | Pub/Sub API gRPC port | `7443` |
| `topic` | (Optional) Default topic path | - |

### Table Options (provided at runtime)

| Option | Description | Default |
|--------|-------------|---------|
| `topic` | Salesforce Pub/Sub topic path (required if not in connection) | - |
| `replayPreset` | Starting point: "EARLIEST" or "LATEST" | `EARLIEST` |
| `pollTimeoutSeconds` | Seconds to wait for events per batch | `10` |
| `maxEventsPerBatch` | Maximum events per batch | `1000` |

**Topic Formats:**
- CDC Events: `/data/{ObjectName}ChangeEvent` (e.g., `/data/AccountChangeEvent`)
- Platform Events: `/event/{EventName}__e` (e.g., `/event/MyPlatformEvent__e`)
- Custom CDC: `/data/{CustomObject}__ChangeEvent` (e.g., `/data/Invoice__ChangeEvent`)

## Topic Configuration

The `topic` parameter can be provided in two ways:

### Option 1: At Connection Level (static)
```python
options = {
    "clientId": "...",
    "clientSecret": "...",
    "topic": "/data/AccountChangeEvent",  # Included in connection
}
connector = LakeflowConnect(options)
connector.read_table("account_change_event", None, {})  # No need to specify topic
```

### Option 2: At Runtime via table_options (dynamic - recommended)
```python
# Connection has auth only
options = {
    "clientId": "...",
    "clientSecret": "...",
}
connector = LakeflowConnect(options)

# Topic provided when reading
connector.read_table(
    "account_change_event",
    None,
    {"topic": "/data/AccountChangeEvent"}  # Topic in table_options
)
```

Option 2 is **recommended** because:
- UC Connection stores only authentication credentials
- Topic can be configured per-pipeline/per-table
- Same connection can be reused for different topics

## Table Naming

The connector derives table names from topic paths:

| Topic | Table Name |
|-------|------------|
| `/data/AccountChangeEvent` | `account_change_event` |
| `/data/ContactChangeEvent` | `contact_change_event` |
| `/event/MyPlatformEvent__e` | `my_platform_event` |
| `/data/CustomObject__ChangeEvent` | `custom_object__change_event` |

## Event Schema

The event table has a fixed schema:

| Column | Type | Description |
|--------|------|-------------|
| `replay_id` | STRING | Unique event identifier for replay (base64-encoded) |
| `event_payload` | BINARY | Raw Avro-encoded event payload |
| `schema_id` | STRING | Avro schema ID for decoding |
| `topic_name` | STRING | Source topic path |
| `timestamp` | LONG | Event receipt timestamp (milliseconds) |
| `decoded_event` | STRING | JSON-decoded event data (includes bitmap field names) |

## Usage Example

### Direct Python Usage

```python
from sources.salesforce_pubsub import LakeflowConnect

# Initialize connector (auth only, no topic)
connector = LakeflowConnect({
    "clientId": "your-client-id",
    "clientSecret": "your-client-secret",
})

# Test connection
result = connector.test_connection()
print(result)  # {'status': 'success', 'message': 'Connection successful...'}

# Specify topic when reading
topic = "/data/AccountChangeEvent"
table_name = "account_change_event"

# Get schema (topic in table_options)
schema = connector.get_table_schema(table_name, {"topic": topic})
print(schema)

# Get metadata
metadata = connector.read_table_metadata(table_name, {"topic": topic})
print(metadata)
# {'primary_keys': ['replay_id'], 'cursor_field': 'replay_id', 'ingestion_type': 'append'}

# Read events
events, offset = connector.read_table(
    table_name,
    start_offset=None,
    table_options={
        "topic": topic,
        "replayPreset": "EARLIEST"
    }
)

for event in events:
    print(event["decoded_event"])

# Read next batch (incremental)
more_events, next_offset = connector.read_table(
    table_name,
    start_offset=offset,
    table_options={"topic": topic}
)
```

### Spark Streaming Usage

```python
# After generating the merged source using scripts/merge_python_source.py

# Register the data source
from _generated_salesforce_pubsub_python_source import register_lakeflow_source
register_lakeflow_source(spark)

# Read from Salesforce Pub/Sub
# UC connection has auth only, topic provided via options
df = spark.readStream.format("lakeflow_connect") \
    .option("databricks.connection", "salesforce_pubsub_connection") \
    .option("topic", "/data/AccountChangeEvent") \
    .option("tableName", "account_change_event") \
    .load()

# Process the stream
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint/account_changes") \
    .table("bronze.account_change_events")
```

## Ingestion Behavior

- **Ingestion Type**: `append` - Events are immutable and ordered by replay_id
- **Checkpointing**: Uses `replay_id` for exactly-once processing
- **Batching**: Collects events for `pollTimeoutSeconds` or until `maxEventsPerBatch` is reached
- **Offset Management**: Each batch returns the latest replay_id as the next offset

## Decoded Event Structure

For CDC events, the `decoded_event` JSON includes:

```json
{
  "ChangeEventHeader": {
    "entityName": "Account",
    "recordIds": ["001xx000003DGXXAA4"],
    "changeType": "UPDATE",
    "changedFields": ["Name", "Industry"],
    "changedFieldNames": ["Name", "Industry"],
    "nulledFields": [],
    "nulledFieldNames": [],
    "diffFields": [],
    "diffFieldNames": [],
    "commitTimestamp": 1704067200000,
    "commitUser": "005xx000001SvMAA0",
    "commitNumber": 12345
  },
  "Name": "Updated Account Name",
  "Industry": "Technology"
}
```

## Running Tests

```bash
# From the repository root
python -m pytest sources/salesforce_pubsub/test/ -v

# Or run directly
python sources/salesforce_pubsub/test/test_salesforce_pubsub_lakeflow_connect.py
```

## Error Handling

- **Missing Topic**: Raises `ValueError` if `topic` is not provided in connection or table_options
- **Table Name Mismatch**: Raises `ValueError` if table name doesn't match the provided topic
- **Authentication Errors**: Raises `ValueError` if credentials are missing or invalid
- **Topic Not Found**: Raises `RuntimeError` if the specified topic doesn't exist
- **Connection Failures**: Raises `RuntimeError` with detailed error message
- **Event Processing Errors**: Logged but don't stop the batch collection
