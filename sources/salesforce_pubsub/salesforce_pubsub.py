"""
Salesforce Pub/Sub Lakeflow Connector

A LakeflowConnect implementation for Salesforce Pub/Sub API that supports
streaming Change Data Capture (CDC) events, Platform Events, and Custom Events.

This is a single-file, self-contained connector that includes all necessary
components for communicating with the Salesforce Pub/Sub API.
"""

import io
import json
import time
import base64
import threading
import warnings
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from typing import Dict, List, Iterator, Any, Optional, Callable

import avro.io
import avro.schema
import certifi
import grpc
import requests
from avro.schema import Schema
from bitstring import BitArray
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BinaryType,
)


# =============================================================================
# PROTOBUF DEFINITIONS (from pubsub_api.proto)
# Lazy-loaded to avoid pickling issues with Spark DataSource registration
# =============================================================================

# Serialized protobuf descriptor - stored as constant, classes built on demand
_PUBSUB_PROTO_DESCRIPTOR = b'\n\x10pubsub_api.proto\x12\x0b\x65ventbus.v1\"\x83\x01\n\tTopicInfo\x12\x12\n\ntopic_name\x18\x01 \x01(\t\x12\x13\n\x0btenant_guid\x18\x02 \x01(\t\x12\x13\n\x0b\x63\x61n_publish\x18\x03 \x01(\x08\x12\x15\n\rcan_subscribe\x18\x04 \x01(\x08\x12\x11\n\tschema_id\x18\x05 \x01(\t\x12\x0e\n\x06rpc_id\x18\x06 \x01(\t\"\"\n\x0cTopicRequest\x12\x12\n\ntopic_name\x18\x01 \x01(\t\")\n\x0b\x45ventHeader\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\"j\n\rProducerEvent\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\tschema_id\x18\x02 \x01(\t\x12\x0f\n\x07payload\x18\x03 \x01(\x0c\x12)\n\x07headers\x18\x04 \x03(\x0b\x32\x18.eventbus.v1.EventHeader\"M\n\rConsumerEvent\x12)\n\x05\x65vent\x18\x01 \x01(\x0b\x32\x1a.eventbus.v1.ProducerEvent\x12\x11\n\treplay_id\x18\x02 \x01(\x0c\"^\n\rPublishResult\x12\x11\n\treplay_id\x18\x01 \x01(\x0c\x12!\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x12.eventbus.v1.Error\x12\x17\n\x0f\x63orrelation_key\x18\x03 \x01(\t\":\n\x05\x45rror\x12$\n\x04\x63ode\x18\x01 \x01(\x0e\x32\x16.eventbus.v1.ErrorCode\x12\x0b\n\x03msg\x18\x02 \x01(\t\"\x94\x01\n\x0c\x46\x65tchRequest\x12\x12\n\ntopic_name\x18\x01 \x01(\t\x12\x30\n\rreplay_preset\x18\x02 \x01(\x0e\x32\x19.eventbus.v1.ReplayPreset\x12\x11\n\treplay_id\x18\x03 \x01(\x0c\x12\x15\n\rnum_requested\x18\x04 \x01(\x05\x12\x14\n\x0c\x61uth_refresh\x18\x05 \x01(\t\"\x84\x01\n\rFetchResponse\x12*\n\x06\x65vents\x18\x01 \x03(\x0b\x32\x1a.eventbus.v1.ConsumerEvent\x12\x18\n\x10latest_replay_id\x18\x02 \x01(\x0c\x12\x0e\n\x06rpc_id\x18\x03 \x01(\t\x12\x1d\n\x15pending_num_requested\x18\x04 \x01(\x05\"\"\n\rSchemaRequest\x12\x11\n\tschema_id\x18\x01 \x01(\t\"D\n\nSchemaInfo\x12\x13\n\x0bschema_json\x18\x01 \x01(\t\x12\x11\n\tschema_id\x18\x02 \x01(\t\x12\x0e\n\x06rpc_id\x18\x03 \x01(\t\"f\n\x0ePublishRequest\x12\x12\n\ntopic_name\x18\x01 \x01(\t\x12*\n\x06\x65vents\x18\x02 \x03(\x0b\x32\x1a.eventbus.v1.ProducerEvent\x12\x14\n\x0c\x61uth_refresh\x18\x03 \x01(\t\"a\n\x0fPublishResponse\x12+\n\x07results\x18\x01 \x03(\x0b\x32\x1a.eventbus.v1.PublishResult\x12\x11\n\tschema_id\x18\x02 \x01(\t\x12\x0e\n\x06rpc_id\x18\x03 \x01(\t\"\xb7\x01\n\x13ManagedFetchRequest\x12\x17\n\x0fsubscription_id\x18\x01 \x01(\t\x12\x16\n\x0e\x64\x65veloper_name\x18\x02 \x01(\t\x12\x15\n\rnum_requested\x18\x03 \x01(\x05\x12\x14\n\x0c\x61uth_refresh\x18\x04 \x01(\t\x12\x42\n\x18\x63ommit_replay_id_request\x18\x05 \x01(\x0b\x32 .eventbus.v1.CommitReplayRequest\"\xc7\x01\n\x14ManagedFetchResponse\x12*\n\x06\x65vents\x18\x01 \x03(\x0b\x32\x1a.eventbus.v1.ConsumerEvent\x12\x18\n\x10latest_replay_id\x18\x02 \x01(\x0c\x12\x0e\n\x06rpc_id\x18\x03 \x01(\t\x12\x1d\n\x15pending_num_requested\x18\x04 \x01(\x05\x12:\n\x0f\x63ommit_response\x18\x05 \x01(\x0b\x32!.eventbus.v1.CommitReplayResponse\"C\n\x13\x43ommitReplayRequest\x12\x19\n\x11\x63ommit_request_id\x18\x01 \x01(\t\x12\x11\n\treplay_id\x18\x02 \x01(\x0c\"}\n\x14\x43ommitReplayResponse\x12\x19\n\x11\x63ommit_request_id\x18\x01 \x01(\t\x12\x11\n\treplay_id\x18\x02 \x01(\x0c\x12!\n\x05\x65rror\x18\x03 \x01(\x0b\x32\x12.eventbus.v1.Error\x12\x14\n\x0cprocess_time\x18\x04 \x01(\x03*1\n\tErrorCode\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0b\n\x07PUBLISH\x10\x01\x12\n\n\x06\x43OMMIT\x10\x02*4\n\x0cReplayPreset\x12\n\n\x06LATEST\x10\x00\x12\x0c\n\x08\x45\x41RLIEST\x10\x01\x12\n\n\x06\x43USTOM\x10\x02\x32\xc4\x03\n\x06PubSub\x12\x46\n\tSubscribe\x12\x19.eventbus.v1.FetchRequest\x1a\x1a.eventbus.v1.FetchResponse(\x01\x30\x01\x12@\n\tGetSchema\x12\x1a.eventbus.v1.SchemaRequest\x1a\x17.eventbus.v1.SchemaInfo\x12=\n\x08GetTopic\x12\x19.eventbus.v1.TopicRequest\x1a\x16.eventbus.v1.TopicInfo\x12\x44\n\x07Publish\x12\x1b.eventbus.v1.PublishRequest\x1a\x1c.eventbus.v1.PublishResponse\x12N\n\rPublishStream\x12\x1b.eventbus.v1.PublishRequest\x1a\x1c.eventbus.v1.PublishResponse(\x01\x30\x01\x12[\n\x10ManagedSubscribe\x12 .eventbus.v1.ManagedFetchRequest\x1a!.eventbus.v1.ManagedFetchResponse(\x01\x30\x01\x42\x61\n com.salesforce.eventbus.protobufB\x0bPubSubProtoP\x01Z.github.com/developerforce/pub-sub-api/go/protob\x06proto3'

# Cache for lazily-initialized protobuf classes
_PROTOBUF_CACHE: Dict[str, Any] = {}


def _get_protobuf_classes() -> Dict[str, Any]:
    """
    Lazily initialize and cache protobuf message classes.
    
    This defers protobuf initialization until actually needed (during gRPC calls),
    avoiding pickling issues when Spark registers the DataSource.
    """
    if _PROTOBUF_CACHE:
        return _PROTOBUF_CACHE
    
    _sym_db = _symbol_database.Default()
    DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(_PUBSUB_PROTO_DESCRIPTOR)
    
    _pb_globals: Dict[str, Any] = {}
    _builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _pb_globals)
    _builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'pubsub_api_pb2', _pb_globals)
    
    # Cache the classes we need
    _PROTOBUF_CACHE['TopicInfo'] = _pb_globals['TopicInfo']
    _PROTOBUF_CACHE['TopicRequest'] = _pb_globals['TopicRequest']
    _PROTOBUF_CACHE['FetchRequest'] = _pb_globals['FetchRequest']
    _PROTOBUF_CACHE['FetchResponse'] = _pb_globals['FetchResponse']
    _PROTOBUF_CACHE['SchemaRequest'] = _pb_globals['SchemaRequest']
    _PROTOBUF_CACHE['SchemaInfo'] = _pb_globals['SchemaInfo']
    _PROTOBUF_CACHE['ReplayPreset'] = _pb_globals['ReplayPreset']
    
    return _PROTOBUF_CACHE


# =============================================================================
# GRPC STUB (from pubsub_api_pb2_grpc.py)
# =============================================================================

class PubSubStub(object):
    """
    The Pub/Sub API provides a single interface for publishing and subscribing to platform events.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        # Get protobuf classes lazily
        pb = _get_protobuf_classes()
        FetchRequest = pb['FetchRequest']
        FetchResponse = pb['FetchResponse']
        SchemaRequest = pb['SchemaRequest']
        SchemaInfo = pb['SchemaInfo']
        TopicRequest = pb['TopicRequest']
        TopicInfo = pb['TopicInfo']
        
        self.Subscribe = channel.stream_stream(
            '/eventbus.v1.PubSub/Subscribe',
            request_serializer=FetchRequest.SerializeToString,
            response_deserializer=FetchResponse.FromString,
            _registered_method=True
        )
        self.GetSchema = channel.unary_unary(
            '/eventbus.v1.PubSub/GetSchema',
            request_serializer=SchemaRequest.SerializeToString,
            response_deserializer=SchemaInfo.FromString,
            _registered_method=True
        )
        self.GetTopic = channel.unary_unary(
            '/eventbus.v1.PubSub/GetTopic',
            request_serializer=TopicRequest.SerializeToString,
            response_deserializer=TopicInfo.FromString,
            _registered_method=True
        )


# =============================================================================
# BITMAP PROCESSOR UTILITIES
# =============================================================================

def _convert_hexbinary_to_bitset(bitmap: str) -> str:
    """Convert hex binary string to bit set representation."""
    bit_array = BitArray(hex=bitmap[2:])
    binary_string = bit_array.bin
    return binary_string[::-1]


def _find_bit_positions(to_find: str, binary_string: str) -> List[int]:
    """Find the positions of a character in the binary string."""
    return [i for i, x in enumerate(binary_string) if x == to_find]


def _get_fieldnames_from_bitstring(bitmap: str, avro_schema: Schema) -> List[str]:
    """Get field names from a bitmap string based on the Avro schema."""
    bitmap_field_name = []
    fields_list = list(avro_schema.fields)
    binary_string = _convert_hexbinary_to_bitset(bitmap)
    indexes = _find_bit_positions('1', binary_string)
    for index in indexes:
        bitmap_field_name.append(fields_list[index].name)
    return bitmap_field_name


def _get_value_schema(parent_field):
    """Get the value type of an 'optional' schema, which is a union of [null, valueSchema]."""
    if parent_field.type == 'union':
        schemas = parent_field.schemas
        if len(schemas) == 2 and schemas[0].type == 'null':
            return schemas[1]
        if len(schemas) == 2 and schemas[0].type == 'string':
            return schemas[1]
        if len(schemas) == 3 and schemas[0].type == 'null' and schemas[1].type == 'string':
            return schemas[2]
    return parent_field


def _append_parent_name(parent_field_name: str, full_field_names: List[str]) -> List[str]:
    """Append parent field name to nested field names."""
    for index in range(len(full_field_names)):
        full_field_names[index] = parent_field_name + "." + full_field_names[index]
    return full_field_names


def process_bitmap(avro_schema: Schema, bitmap_fields: list) -> List[str]:
    """
    Process bitmap fields from a Salesforce CDC event and convert them to field names.

    Args:
        avro_schema: The Avro schema for the event
        bitmap_fields: List of bitmap field values (hex strings or parent-child mappings)

    Returns:
        List of field names represented by the bitmap
    """
    fields = []
    if len(bitmap_fields) != 0:
        # Replace top field level bitmap with list of fields
        if bitmap_fields[0].startswith("0x"):
            bitmap = bitmap_fields[0]
            fields = fields + _get_fieldnames_from_bitstring(bitmap, avro_schema)
            bitmap_fields.remove(bitmap)

        # Replace parentPos-nested Nulled BitMap with list of fields too
        if len(bitmap_fields) != 0 and "-" in str(bitmap_fields[-1]):
            for bitmap_field in bitmap_fields:
                if bitmap_field is not None and "-" in str(bitmap_field):
                    bitmap_strings = bitmap_field.split("-")
                    # Interpret the parent field name from mapping of parentFieldPos -> childFieldbitMap
                    parent_field = avro_schema.fields[int(bitmap_strings[0])]
                    child_schema = _get_value_schema(parent_field.type)
                    # Make sure we're really dealing with compound field
                    if child_schema.type is not None and child_schema.type == 'record':
                        parent_field_name = parent_field.name
                        # Interpret the child field names from mapping of parentFieldPos -> childFieldbitMap
                        full_field_names = _get_fieldnames_from_bitstring(bitmap_strings[1], child_schema)
                        full_field_names = _append_parent_name(parent_field_name, full_field_names)
                        if len(full_field_names) > 0:
                            fields = fields + full_field_names
    return fields


# =============================================================================
# PUBSUB API CLIENT
# =============================================================================

class PubSubAPIClient:
    """
    Salesforce Pub/Sub API Client for Lakeflow connector.

    This client supports both OAuth Client Credentials and Password authentication.
    """

    def __init__(self, grpc_host: str = 'api.pubsub.salesforce.com', grpc_port: int = 7443):
        """
        Initialize the PubSub API client.

        Args:
            grpc_host: gRPC server host
            grpc_port: gRPC server port
        """
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port

        # Auth fields - will be set during authentication
        self.session_token: Optional[str] = None
        self.instance_url: Optional[str] = None
        self.tenant_id: Optional[str] = None
        self.schema_cache: Dict[str, Any] = {}

        # gRPC connection objects (initialized lazily)
        self.channel = None
        self.stub = None
        self._connection_initialized = False

    def _ensure_connection(self):
        """Lazily initialize gRPC connection."""
        if not self._connection_initialized:
            try:
                with open(certifi.where(), 'rb') as f:
                    creds = grpc.ssl_channel_credentials(f.read())

                self.channel = grpc.secure_channel(f'{self.grpc_host}:{self.grpc_port}', creds)
                self.stub = PubSubStub(self.channel)
                self._connection_initialized = True

            except Exception as e:
                raise RuntimeError(f"Failed to initialize gRPC connection: {e}")

    def __getstate__(self):
        """Custom pickle behavior to exclude non-serializable gRPC objects."""
        state = self.__dict__.copy()
        state['channel'] = None
        state['stub'] = None
        state['_connection_initialized'] = False
        return state

    def __setstate__(self, state):
        """Custom unpickle behavior to restore serializable state."""
        self.__dict__.update(state)

    def authenticate(
        self,
        username: str,
        password: str,
        security_token: Optional[str] = None,
        login_url: str = 'https://login.salesforce.com'
    ) -> bool:
        """
        Authenticate with Salesforce using password credentials.

        Args:
            username: Salesforce username
            password: Salesforce password
            security_token: Security token (can be None if included in password)
            login_url: Salesforce login URL

        Returns:
            True if authentication successful
        """
        try:
            full_password = password
            if security_token:
                full_password = password + security_token

            # SOAP login request
            soap_body = (
                "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
                "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>"
                "<urn:login><urn:username><![CDATA[" + username +
                "]]></urn:username><urn:password><![CDATA[" + full_password +
                "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
            )

            headers = {'Content-Type': 'text/xml', 'SOAPAction': 'Login'}

            response = requests.post(
                f'{login_url}/services/Soap/u/60.0',
                data=soap_body,
                headers=headers
            )

            if response.status_code == 200:
                root = ET.fromstring(response.text)
                res_xml = root[0][0][0]  # Body -> loginResponse -> result

                server_url = res_xml[3].text
                url_parts = urlparse(server_url)
                self.instance_url = f"{url_parts.scheme}://{url_parts.netloc}"
                self.session_token = res_xml[4].text
                uinfo = res_xml[6]
                self.tenant_id = uinfo[8].text

                return True
            else:
                print(f"Authentication failed: {response.status_code}")
                return False

        except Exception as e:
            print(f"Authentication error: {e}")
            return False

    def authenticate_with_client_credentials(
        self,
        client_id: str,
        client_secret: str,
        login_url: str = 'https://login.salesforce.com'
    ) -> bool:
        """
        Authenticate using OAuth 2.0 Client Credentials flow.

        Args:
            client_id: Connected App Consumer Key
            client_secret: Connected App Consumer Secret
            login_url: Salesforce login URL

        Returns:
            True if authentication successful
        """
        try:
            response = requests.post(
                f'{login_url}/services/oauth2/token',
                data={
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret
                }
            )

            if response.status_code == 200:
                token_data = response.json()
                self.session_token = token_data['access_token']
                self.instance_url = token_data['instance_url']
                self.tenant_id = self._fetch_tenant_id()
                return True
            else:
                print(f"Client credentials auth failed: {response.status_code}")
                return False

        except Exception as e:
            print(f"Client credentials auth error: {e}")
            return False

    def _fetch_tenant_id(self) -> Optional[str]:
        """Fetch tenant/org ID from Salesforce userinfo endpoint."""
        try:
            headers = {'Authorization': f'Bearer {self.session_token}'}
            response = requests.get(
                f'{self.instance_url}/services/oauth2/userinfo',
                headers=headers
            )

            if response.status_code == 200:
                userinfo = response.json()
                return userinfo.get('organization_id')
            else:
                raise RuntimeError(f"Failed to fetch tenant ID: {response.status_code}")

        except Exception as e:
            print(f"Could not fetch tenant ID: {e}")
            return None

    def get_auth_metadata(self):
        """Get gRPC auth metadata for authenticated requests."""
        if not self.session_token:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")

        return [
            ('accesstoken', self.session_token),
            ('instanceurl', self.instance_url),
            ('tenantid', self.tenant_id)
        ]

    def get_topic(self, topic_name: str):
        """
        Get topic information from Salesforce.

        Args:
            topic_name: Name of the topic (e.g., '/data/AccountChangeEvent')

        Returns:
            TopicInfo or None if error
        """
        try:
            self._ensure_connection()
            TopicRequest = _get_protobuf_classes()['TopicRequest']
            request = TopicRequest(topic_name=topic_name)
            return self.stub.GetTopic(request, metadata=self.get_auth_metadata())
        except grpc.RpcError as e:
            print(f"Error getting topic {topic_name}: {e}")
            return None

    def get_schema(self, schema_id: str):
        """
        Get and cache Avro schema by ID.

        Args:
            schema_id: Schema ID from Salesforce

        Returns:
            Parsed Avro schema or None if error
        """
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]

        try:
            self._ensure_connection()
            SchemaRequest = _get_protobuf_classes()['SchemaRequest']
            request = SchemaRequest(schema_id=schema_id)
            response = self.stub.GetSchema(request, metadata=self.get_auth_metadata())

            schema_json = json.loads(response.schema_json)
            schema = avro.schema.parse(json.dumps(schema_json))
            self.schema_cache[schema_id] = schema
            return schema

        except grpc.RpcError as e:
            print(f"Error getting schema {schema_id}: {e}")
            return None
        except Exception as e:
            print(f"Error parsing schema {schema_id}: {e}")
            return None

    def subscribe(
        self,
        topic_name: str,
        callback: Optional[Callable] = None,
        replay_preset=None,
        replay_id_binary: Optional[bytes] = None,
        num_requested: int = 50
    ) -> Optional[threading.Thread]:
        """
        Subscribe to topic events.

        Args:
            topic_name: Topic to subscribe to
            callback: Custom event handler function
            replay_preset: Replay preset (defaults to EARLIEST)
            replay_id_binary: Binary replay ID to start from
            num_requested: Number of events to request per batch

        Returns:
            Subscription thread
        """
        self._ensure_connection()

        topic_info = self.get_topic(topic_name)
        if not topic_info:
            print(f"Topic {topic_name} not found")
            return None

        # Get protobuf classes lazily
        pb = _get_protobuf_classes()
        ReplayPreset = pb['ReplayPreset']
        FetchRequest = pb['FetchRequest']

        if replay_preset is None:
            replay_preset = ReplayPreset.EARLIEST

        subscription_semaphore = threading.Semaphore(1)

        def fetch_req_stream():
            """Generate FetchRequests when needed."""
            while True:
                subscription_semaphore.acquire()

                if replay_id_binary:
                    yield FetchRequest(
                        topic_name=topic_name,
                        replay_preset=ReplayPreset.CUSTOM,
                        replay_id=replay_id_binary,
                        num_requested=num_requested
                    )
                else:
                    yield FetchRequest(
                        topic_name=topic_name,
                        replay_preset=replay_preset,
                        num_requested=num_requested
                    )

        def handle_events():
            """Process subscription events."""
            try:
                event_stream = self.stub.Subscribe(
                    fetch_req_stream(),
                    metadata=self.get_auth_metadata()
                )

                for event in event_stream:
                    if event.events:
                        for received_event in event.events:
                            if callback:
                                callback(received_event)
                            else:
                                self._default_event_handler(received_event, topic_info.schema_id)

                    subscription_semaphore.release()

            except grpc.RpcError as e:
                print(f"Subscription error for {topic_name}: {e}")
            except Exception as e:
                print(f"Unexpected subscription error for {topic_name}: {e}")

        thread = threading.Thread(target=handle_events, name=f"PubSub-{topic_name}")
        thread.daemon = True
        thread.start()

        subscription_semaphore.release()

        return thread

    def _default_event_handler(self, event, schema_id: str):
        """Default event handler with decoding."""
        try:
            print(f"\nEvent received!")
            print(f"   Replay ID: {event.replay_id}")
            print(f"   Time: {time.strftime('%H:%M:%S')}")

            schema = self.get_schema(schema_id)
            if schema:
                reader = avro.io.DatumReader(schema)
                binary_data = io.BytesIO(event.event.payload)
                decoder = avro.io.BinaryDecoder(binary_data)
                decoded_event = reader.read(decoder)
                print(f"   Event data: {json.dumps(decoded_event, indent=2)}")
            else:
                print("   Could not decode event")

        except Exception as e:
            print(f"   Error processing event: {e}")

    def test_connection(self) -> bool:
        """Test the connection and authentication status."""
        if not self.session_token:
            return False

        try:
            self._ensure_connection()
            test_topic = "/data/AccountChangeEvent"
            topic_info = self.get_topic(test_topic)
            return topic_info is not None
        except Exception:
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information for debugging."""
        return {
            'grpc_host': self.grpc_host,
            'grpc_port': self.grpc_port,
            'instance_url': self.instance_url,
            'tenant_id': self.tenant_id,
            'authenticated': bool(self.session_token),
            'connection_initialized': self._connection_initialized,
            'schema_cache_size': len(self.schema_cache)
        }

    def close(self):
        """Close gRPC channel and cleanup resources."""
        if self.channel:
            self.channel.close()

        self.channel = None
        self.stub = None
        self._connection_initialized = False
        self.session_token = None
        self.instance_url = None
        self.tenant_id = None
        self.schema_cache.clear()


def create_client_from_options(options: Dict[str, str]) -> PubSubAPIClient:
    """
    Create a PubSubAPIClient from connection options.

    Supports multiple authentication methods:
    - OAuth Client Credentials: clientId + clientSecret (preferred)
    - Password: username + password (legacy)

    Args:
        options: Connection options dictionary

    Returns:
        Configured and authenticated client
    """
    grpc_host = options.get('grpcHost', 'api.pubsub.salesforce.com')
    grpc_port = int(options.get('grpcPort', 7443))
    login_url = options.get('loginUrl', 'https://login.salesforce.com')

    client = PubSubAPIClient(grpc_host=grpc_host, grpc_port=grpc_port)

    # Check for OAuth Client Credentials (preferred)
    client_id = options.get('clientId')
    client_secret = options.get('clientSecret')

    if client_id and client_secret:
        if not client.authenticate_with_client_credentials(
            client_id=client_id,
            client_secret=client_secret,
            login_url=login_url
        ):
            raise RuntimeError("Failed to authenticate with Salesforce using OAuth Client Credentials")
        return client

    # Fallback to password authentication (legacy)
    username = options.get('username')
    password = options.get('password')

    if username and password:
        if not client.authenticate(username=username, password=password, login_url=login_url):
            raise RuntimeError("Failed to authenticate with Salesforce using password")
        return client

    raise ValueError(
        "Authentication credentials required. Provide either:\n"
        "  - clientId + clientSecret (OAuth Client Credentials), or\n"
        "  - username + password (legacy)"
    )


# =============================================================================
# LAKEFLOW CONNECT IMPLEMENTATION
# =============================================================================


class LakeflowConnect:
    """
    Lakeflow connector for Salesforce Pub/Sub API.

    This connector enables streaming ingestion of Salesforce events including:
    - Change Data Capture (CDC) events from Salesforce objects
    - Platform Events published to Salesforce
    - Custom Events with user-defined schemas

    Authentication Options (provide one set in options):
        OAuth Client Credentials (recommended):
        - clientId: Connected App Consumer Key
        - clientSecret: Connected App Consumer Secret

        Password (legacy):
        - username: Salesforce username
        - password: Salesforce password + security token

    Connection Options:
        - loginUrl: Salesforce login URL (default: https://login.salesforce.com)
        - grpcHost: PubSub API host (default: api.pubsub.salesforce.com)
        - grpcPort: PubSub API port (default: 7443)

    Topic Options (can be provided at connection level OR via table_options):
        - topic: Salesforce Pub/Sub topic path
                 e.g., "/data/AccountChangeEvent" or "/event/MyPlatformEvent__e"
                 Required before calling read_table, but can be deferred to table_options.

    Streaming Options:
        - pollTimeoutSeconds: How long to wait for events per batch (default: 10)
        - maxEventsPerBatch: Maximum events to return per batch (default: 1000)
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Salesforce Pub/Sub connector.

        Args:
            options: Connection options including authentication credentials
        """
        self.options = options

        # Authentication options
        self.client_id = options.get("clientId")
        self.client_secret = options.get("clientSecret")
        self.username = options.get("username")
        self.password = options.get("password")

        # Connection options
        self.login_url = options.get("loginUrl", "https://login.salesforce.com")
        self.grpc_host = options.get("grpcHost", "api.pubsub.salesforce.com")
        self.grpc_port = int(options.get("grpcPort", "7443"))

        # Streaming options
        self.poll_timeout_seconds = int(options.get("pollTimeoutSeconds", "10"))
        self.max_events_per_batch = int(options.get("maxEventsPerBatch", "1000"))

        # Topic configuration (optional at init, can be provided via table_options)
        self._connection_topic = options.get("topic", "").strip() or None

        # Validate authentication
        has_oauth = self.client_id and self.client_secret
        has_password = self.username and self.password
        if not has_oauth and not has_password:
            raise ValueError(
                "Authentication credentials required. Provide either:\n"
                "  - clientId + clientSecret (OAuth Client Credentials), or\n"
                "  - username + password (legacy)"
            )

        # Client state (initialized lazily)
        self._client: Optional[PubSubAPIClient] = None
        self._client_initialized = False

        # Schema cache
        self._schema_cache: Dict[str, StructType] = {}
        self._topic_info_cache: Dict[str, Any] = {}

    def _ensure_client_initialized(self) -> PubSubAPIClient:
        """
        Lazily initialize and authenticate the PubSub client.

        Returns:
            Authenticated PubSubAPIClient instance
        """
        if not self._client_initialized:
            try:
                self._client = create_client_from_options(self.options)
                self._client_initialized = True
            except Exception as e:
                raise RuntimeError(
                    f"Failed to initialize Salesforce Pub/Sub client: {e}"
                )

        return self._client

    def _topic_to_table_name(self, topic: str) -> str:
        """
        Convert a Salesforce topic path to a table name.

        Examples:
            /data/AccountChangeEvent -> account_change_event
            /event/MyPlatformEvent__e -> my_platform_event
        """
        # Remove leading path components
        name = topic.split("/")[-1]

        # Remove __e suffix for platform events
        if name.endswith("__e"):
            name = name[:-3]

        # Convert CamelCase to snake_case
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append("_")
            result.append(char.lower())

        return "".join(result)

    def _resolve_topic(self, table_options: Dict[str, str]) -> str:
        """
        Resolve the topic from table_options or connection options.

        Args:
            table_options: Table-specific options that may contain 'topic'

        Returns:
            The resolved topic path

        Raises:
            ValueError: If no topic is provided in either location
        """
        # First check table_options (allows override)
        topic = table_options.get("topic", "").strip()

        # Fall back to connection-level topic
        if not topic:
            topic = self._connection_topic

        if not topic:
            raise ValueError(
                "Topic is required. Provide 'topic' in table_options or connection options:\n"
                "  - topic: '/data/AccountChangeEvent' (for CDC events), or\n"
                "  - topic: '/event/MyPlatformEvent__e' (for Platform Events)"
            )

        return topic

    def list_tables(self) -> List[str]:
        """
        List available tables.

        If a topic was configured at connection level, returns the derived table name.
        Otherwise, returns a placeholder indicating topic must be provided.

        Returns:
            List containing the table name(s)
        """
        if self._connection_topic:
            return [self._topic_to_table_name(self._connection_topic)]
        # When no topic is configured, return empty - topic will be provided via table_options
        return []

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the schema for a Salesforce Pub/Sub event table.

        All Pub/Sub event tables share a common schema structure that includes
        the raw event data along with metadata for replay and decoding.

        Args:
            table_name: Name of the table (derived from topic)
            table_options: Additional table-specific options (may contain 'topic')

        Returns:
            StructType schema for the event table
        """
        # Resolve topic (validates it's provided)
        topic = self._resolve_topic(table_options)
        expected_table_name = self._topic_to_table_name(topic)

        # Validate table name matches the topic
        if table_name != expected_table_name:
            raise ValueError(
                f"Table name '{table_name}' does not match topic '{topic}'. "
                f"Expected table name: '{expected_table_name}'"
            )

        # Check cache
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Standard Pub/Sub event schema
        schema = StructType(
            [
                StructField("replay_id", StringType(), True),
                StructField("event_payload", BinaryType(), True),
                StructField("schema_id", StringType(), True),
                StructField("topic_name", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("decoded_event", StringType(), True),  # JSON string
            ]
        )

        # Cache the schema
        self._schema_cache[table_name] = schema

        return schema

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Get metadata for a Salesforce Pub/Sub event table.

        Args:
            table_name: Name of the table
            table_options: Additional table-specific options (may contain 'topic')

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        # Resolve topic (validates it's provided)
        topic = self._resolve_topic(table_options)
        expected_table_name = self._topic_to_table_name(topic)

        # Validate table name matches the topic
        if table_name != expected_table_name:
            raise ValueError(
                f"Table name '{table_name}' does not match topic '{topic}'. "
                f"Expected table name: '{expected_table_name}'"
            )

        # All Pub/Sub tables use the same metadata structure
        # - replay_id is the unique identifier and ordering cursor
        # - ingestion_type is "append" since events are immutable and ordered
        return {
            "primary_keys": ["replay_id"],
            "cursor_field": "replay_id",
            "ingestion_type": "append",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read events from a Salesforce Pub/Sub topic.

        This method subscribes to the topic, collects events for the configured
        poll timeout, and returns them as a batch with the latest replay_id
        as the next offset.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing the replay_id to start from
            table_options: Additional options for the read:
                - topic: Salesforce Pub/Sub topic path (required if not in connection)
                - replayPreset: "EARLIEST" or "LATEST" (default: EARLIEST)
                - pollTimeoutSeconds: Override default poll timeout
                - maxEventsPerBatch: Override default max events

        Returns:
            Tuple of (event_iterator, next_offset)
        """
        # Resolve topic from table_options or connection
        topic = self._resolve_topic(table_options)
        expected_table_name = self._topic_to_table_name(topic)

        # Validate table name matches the topic
        if table_name != expected_table_name:
            raise ValueError(
                f"Table name '{table_name}' does not match topic '{topic}'. "
                f"Expected table name: '{expected_table_name}'"
            )

        # Get read options
        poll_timeout = int(
            table_options.get("pollTimeoutSeconds", str(self.poll_timeout_seconds))
        )
        max_events = int(
            table_options.get("maxEventsPerBatch", str(self.max_events_per_batch))
        )
        replay_preset = table_options.get("replayPreset", "EARLIEST")

        # Initialize client
        client = self._ensure_client_initialized()

        # Determine starting replay ID
        start_replay_id_b64 = None
        if start_offset and start_offset.get("replay_id"):
            start_replay_id_b64 = start_offset["replay_id"]

        # Collect events from subscription
        events = self._collect_events(
            client=client,
            topic=topic,
            start_replay_id_b64=start_replay_id_b64,
            replay_preset=replay_preset,
            poll_timeout=poll_timeout,
            max_events=max_events,
        )

        # Determine next offset
        next_offset = start_offset or {}
        if events:
            # Use the last event's replay_id as the next offset
            last_event = events[-1]
            next_offset = {"replay_id": last_event.get("replay_id")}

        return iter(events), next_offset

    def _collect_events(
        self,
        client: PubSubAPIClient,
        topic: str,
        start_replay_id_b64: Optional[str],
        replay_preset: str,
        poll_timeout: int,
        max_events: int,
    ) -> List[Dict[str, Any]]:
        """
        Collect events from a Pub/Sub subscription.

        This method creates a temporary subscription, collects events for
        the specified timeout, and returns them as a list.

        Args:
            client: Authenticated PubSubAPIClient
            topic: Salesforce topic path
            start_replay_id_b64: Base64-encoded replay ID to start from
            replay_preset: "EARLIEST" or "LATEST"
            poll_timeout: Seconds to wait for events
            max_events: Maximum number of events to collect

        Returns:
            List of event dictionaries
        """
        events = []
        events_lock = threading.Lock()
        stop_event = threading.Event()

        # Get topic info for schema
        topic_info = client.get_topic(topic)
        if not topic_info:
            raise RuntimeError(f"Topic not found: {topic}")

        schema_id = topic_info.schema_id
        avro_schema = client.get_schema(schema_id) if schema_id else None

        def event_callback(received_event):
            """Callback to process incoming events."""
            nonlocal events

            if stop_event.is_set():
                return

            with events_lock:
                if len(events) >= max_events:
                    stop_event.set()
                    return

                try:
                    # Encode replay ID for storage
                    replay_id_b64 = base64.b64encode(
                        received_event.replay_id
                    ).decode("ascii")

                    # Decode the event payload
                    decoded_event = None
                    if avro_schema:
                        try:
                            reader = avro.io.DatumReader(avro_schema)
                            binary_data = io.BytesIO(received_event.event.payload)
                            decoder = avro.io.BinaryDecoder(binary_data)
                            decoded_event = reader.read(decoder)

                            # Decode bitmap fields if present
                            if decoded_event and "ChangeEventHeader" in decoded_event:
                                header = decoded_event["ChangeEventHeader"]
                                for field in [
                                    "changedFields",
                                    "nulledFields",
                                    "diffFields",
                                ]:
                                    if field in header and header[field]:
                                        field_names = process_bitmap(
                                            avro_schema, header[field]
                                        )
                                        header[f"{field}Names"] = field_names
                        except Exception:
                            pass  # Continue without decoded event

                    event_record = {
                        "replay_id": replay_id_b64,
                        "event_payload": received_event.event.payload,
                        "schema_id": schema_id,
                        "topic_name": topic,
                        "timestamp": int(time.time() * 1000),
                        "decoded_event": (
                            json.dumps(decoded_event) if decoded_event else None
                        ),
                    }

                    events.append(event_record)

                except Exception as e:
                    print(f"Error processing event: {e}")

        # Parse replay preset
        ReplayPreset = _get_protobuf_classes()['ReplayPreset']
        if replay_preset.upper() == "LATEST":
            preset = ReplayPreset.LATEST
        else:
            preset = ReplayPreset.EARLIEST

        # Decode start replay ID if provided
        replay_id_binary = None
        if start_replay_id_b64:
            replay_id_binary = base64.b64decode(start_replay_id_b64)

        # Start subscription
        subscription_thread = client.subscribe(
            topic_name=topic,
            callback=event_callback,
            replay_preset=preset,
            replay_id_binary=replay_id_binary,
        )

        if not subscription_thread:
            raise RuntimeError(f"Failed to subscribe to topic: {topic}")

        # Wait for events or timeout
        start_time = time.time()
        while (time.time() - start_time) < poll_timeout:
            if stop_event.is_set():
                break
            time.sleep(0.1)

        # Signal to stop collecting
        stop_event.set()

        # Give a moment for any in-flight events
        time.sleep(0.5)

        return events

    def test_connection(self, topic: Optional[str] = None) -> Dict[str, Any]:
        """
        Test the connection to Salesforce Pub/Sub API.

        Args:
            topic: Optional topic to test access to. If not provided, uses
                   connection-level topic or just tests authentication.

        Returns:
            Dictionary with connection status and details
        """
        try:
            client = self._ensure_client_initialized()

            # Determine which topic to test (if any)
            test_topic = topic or self._connection_topic

            if test_topic:
                # Try to get info on the specified topic
                topic_info = client.get_topic(test_topic)
                if topic_info:
                    return {
                        "status": "success",
                        "message": f"Connection successful. Topic '{test_topic}' is accessible.",
                        "topic": test_topic,
                        "table_name": self._topic_to_table_name(test_topic),
                        "connection_info": client.get_connection_info(),
                    }

            # Fallback: just check if client is authenticated
            if client.test_connection():
                result = {
                    "status": "success",
                    "message": "Connection successful (authentication verified)",
                    "connection_info": client.get_connection_info(),
                }
                if not test_topic:
                    result["note"] = "No topic configured. Provide 'topic' in table_options when reading."
                return result
            else:
                return {
                    "status": "error",
                    "message": "Connection test failed",
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection failed: {str(e)}",
            }
