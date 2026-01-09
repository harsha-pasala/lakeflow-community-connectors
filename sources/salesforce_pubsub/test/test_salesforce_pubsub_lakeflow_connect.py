"""
Tests for Salesforce Pub/Sub Lakeflow Connector

This module contains both:
1. Unit tests that run without Salesforce credentials
2. Integration tests using the LakeflowConnectTester suite (requires credentials)
"""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directories to path for imports
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.salesforce_pubsub.salesforce_pubsub import LakeflowConnect


# =============================================================================
# UNIT TESTS (no credentials required)
# =============================================================================


class TestSalesforcePubSubConnectorUnit(unittest.TestCase):
    """Unit tests for SalesforcePubSubConnector that don't require credentials."""

    def test_init_with_oauth_credentials_no_topic(self):
        """Test initialization with OAuth credentials but no topic (valid)."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        self.assertEqual(connector.client_id, "test-client-id")
        self.assertEqual(connector.client_secret, "test-client-secret")
        self.assertIsNone(connector._connection_topic)
        self.assertIsNone(connector.username)
        self.assertIsNone(connector.password)

    def test_init_with_oauth_credentials_and_topic(self):
        """Test initialization with OAuth credentials and topic."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        self.assertEqual(connector.client_id, "test-client-id")
        self.assertEqual(connector._connection_topic, "/data/AccountChangeEvent")

    def test_init_with_password_credentials(self):
        """Test initialization with password credentials."""
        options = {
            "username": "test@example.com",
            "password": "test-password-token",
        }
        connector = LakeflowConnect(options)

        self.assertEqual(connector.username, "test@example.com")
        self.assertEqual(connector.password, "test-password-token")
        self.assertIsNone(connector.client_id)
        self.assertIsNone(connector.client_secret)

    def test_init_without_credentials_raises_error(self):
        """Test that initialization without credentials raises ValueError."""
        options = {}

        with self.assertRaises(ValueError) as context:
            LakeflowConnect(options)

        self.assertIn("Authentication credentials required", str(context.exception))

    def test_init_with_custom_options(self):
        """Test initialization with custom connection options."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "loginUrl": "https://test.salesforce.com",
            "grpcHost": "custom.pubsub.salesforce.com",
            "grpcPort": "8443",
            "pollTimeoutSeconds": "30",
            "maxEventsPerBatch": "500",
        }
        connector = LakeflowConnect(options)

        self.assertEqual(connector.login_url, "https://test.salesforce.com")
        self.assertEqual(connector.grpc_host, "custom.pubsub.salesforce.com")
        self.assertEqual(connector.grpc_port, 8443)
        self.assertEqual(connector.poll_timeout_seconds, 30)
        self.assertEqual(connector.max_events_per_batch, 500)

    def test_init_with_lowercase_options(self):
        """Test initialization with lowercase option keys (Databricks compatibility)."""
        options = {
            "clientid": "test-client-id",
            "clientsecret": "test-client-secret",
            "loginurl": "https://test.salesforce.com",
        }
        connector = LakeflowConnect(options)

        self.assertEqual(connector.client_id, "test-client-id")
        self.assertEqual(connector.client_secret, "test-client-secret")
        self.assertEqual(connector.login_url, "https://test.salesforce.com")

    def test_list_tables_with_connection_topic(self):
        """Test list_tables returns table when topic is in connection."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        tables = connector.list_tables()

        self.assertEqual(tables, ["account_change_event"])

    def test_list_tables_without_connection_topic(self):
        """Test list_tables returns empty when no topic in connection."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        tables = connector.list_tables()

        self.assertEqual(tables, [])

    def test_topic_to_table_name_cdc_events(self):
        """Test conversion of various CDC topics to table names."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        test_cases = [
            ("/data/AccountChangeEvent", "account_change_event"),
            ("/data/ContactChangeEvent", "contact_change_event"),
            ("/data/LeadChangeEvent", "lead_change_event"),
            ("/data/OpportunityChangeEvent", "opportunity_change_event"),
            ("/data/CaseChangeEvent", "case_change_event"),
            ("/data/TaskChangeEvent", "task_change_event"),
            ("/data/EventChangeEvent", "event_change_event"),
        ]

        for topic, expected in test_cases:
            with self.subTest(topic=topic):
                result = connector._topic_to_table_name(topic)
                self.assertEqual(result, expected)

    def test_topic_to_table_name_platform_event(self):
        """Test conversion of platform event topic to table name."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        result = connector._topic_to_table_name("/event/MyPlatformEvent__e")
        self.assertEqual(result, "my_platform_event")

    def test_resolve_topic_from_table_options(self):
        """Test that topic can be resolved from table_options."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        topic = connector._resolve_topic({"topic": "/data/ContactChangeEvent"})
        self.assertEqual(topic, "/data/ContactChangeEvent")

    def test_resolve_topic_from_connection(self):
        """Test that topic falls back to connection options."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        topic = connector._resolve_topic({})
        self.assertEqual(topic, "/data/AccountChangeEvent")

    def test_resolve_topic_table_options_overrides_connection(self):
        """Test that table_options topic overrides connection topic."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        topic = connector._resolve_topic({"topic": "/data/ContactChangeEvent"})
        self.assertEqual(topic, "/data/ContactChangeEvent")

    def test_resolve_topic_raises_when_missing(self):
        """Test that _resolve_topic raises error when no topic available."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        with self.assertRaises(ValueError) as context:
            connector._resolve_topic({})

        self.assertIn("Topic is required", str(context.exception))

    def test_get_table_schema_with_topic_in_table_options(self):
        """Test get_table_schema with topic provided via table_options."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema(
            "account_change_event", {"topic": "/data/AccountChangeEvent"}
        )

        field_names = [field.name for field in schema.fields]
        self.assertIn("replay_id", field_names)
        self.assertIn("event_payload", field_names)
        self.assertIn("schema_id", field_names)
        self.assertIn("topic_name", field_names)
        self.assertIn("timestamp", field_names)
        self.assertIn("decoded_event", field_names)

    def test_get_table_schema_with_connection_topic(self):
        """Test get_table_schema with topic from connection."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("account_change_event", {})

        field_names = [field.name for field in schema.fields]
        self.assertIn("replay_id", field_names)

    def test_get_table_schema_wrong_table_name(self):
        """Test get_table_schema raises error when table name doesn't match topic."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        with self.assertRaises(ValueError) as context:
            connector.get_table_schema("wrong_table_name", {})

        self.assertIn("does not match topic", str(context.exception))

    def test_get_table_schema_caches_result(self):
        """Test that get_table_schema caches the schema."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        schema1 = connector.get_table_schema("account_change_event", {})
        schema2 = connector.get_table_schema("account_change_event", {})

        self.assertIs(schema1, schema2)

    def test_read_table_metadata_with_table_options_topic(self):
        """Test read_table_metadata with topic in table_options."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
        }
        connector = LakeflowConnect(options)

        metadata = connector.read_table_metadata(
            "account_change_event", {"topic": "/data/AccountChangeEvent"}
        )

        self.assertEqual(metadata["primary_keys"], ["replay_id"])
        self.assertEqual(metadata["cursor_field"], "replay_id")
        self.assertEqual(metadata["ingestion_type"], "append")

    def test_read_table_metadata_wrong_table_name(self):
        """Test read_table_metadata raises error for wrong table name."""
        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        with self.assertRaises(ValueError) as context:
            connector.read_table_metadata("wrong_table", {})

        self.assertIn("does not match topic", str(context.exception))

    def test_schema_field_types(self):
        """Test that schema has correct field types."""
        from pyspark.sql.types import StringType, BinaryType, LongType

        options = {
            "clientId": "test-client-id",
            "clientSecret": "test-client-secret",
            "topic": "/data/AccountChangeEvent",
        }
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("account_change_event", {})

        # Check specific field types
        field_types = {field.name: type(field.dataType) for field in schema.fields}

        self.assertEqual(field_types["replay_id"], StringType)
        self.assertEqual(field_types["event_payload"], BinaryType)
        self.assertEqual(field_types["schema_id"], StringType)
        self.assertEqual(field_types["topic_name"], StringType)
        self.assertEqual(field_types["timestamp"], LongType)
        self.assertEqual(field_types["decoded_event"], StringType)


# =============================================================================
# INTEGRATION TESTS (requires Salesforce credentials)
# =============================================================================


class TestSalesforcePubSubConnectorIntegration(unittest.TestCase):
    """
    Integration tests for SalesforcePubSubConnector.

    These tests require valid Salesforce credentials and are skipped
    if credentials are not provided via config files.
    """

    @classmethod
    def setUpClass(cls):
        """Load test configuration."""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "configs",
            "dev_config.json",
        )
        table_config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "configs",
            "dev_table_config.json",
        )

        cls.config = {}
        cls.table_config = {}

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                cls.config = json.load(f)

        if os.path.exists(table_config_path):
            with open(table_config_path, "r") as f:
                cls.table_config = json.load(f)

        # Check for required credentials
        cls.has_oauth = cls.config.get("clientId") and cls.config.get("clientSecret")
        cls.has_password = cls.config.get("username") and cls.config.get("password")
        cls.has_credentials = cls.has_oauth or cls.has_password

    def setUp(self):
        """Skip if no credentials."""
        if not self.has_credentials:
            self.skipTest("Salesforce credentials not configured in dev_config.json")

    def test_connection_without_topic(self):
        """Test connection to Salesforce without topic (auth only)."""
        config = {k: v for k, v in self.config.items() if k != "topic"}
        connector = LakeflowConnect(config)
        result = connector.test_connection()

        self.assertEqual(result["status"], "success")

    def test_connection_with_topic(self):
        """Test connection with topic verification."""
        config = dict(self.config)
        if "topic" not in config:
            config["topic"] = "/data/AccountChangeEvent"

        connector = LakeflowConnect(config)
        result = connector.test_connection()

        self.assertEqual(result["status"], "success")
        if config.get("topic"):
            self.assertIn("topic", result)

    def test_read_events_with_topic_in_table_options(self):
        """Test reading events with topic provided in table_options."""
        config = {k: v for k, v in self.config.items() if k != "topic"}
        connector = LakeflowConnect(config)

        topic = "/data/AccountChangeEvent"
        table_name = connector._topic_to_table_name(topic)

        events, offset = connector.read_table(
            table_name,
            start_offset=None,
            table_options={
                "topic": topic,
                "replayPreset": "LATEST",
                "pollTimeoutSeconds": "5",
            },
        )

        events_list = list(events)
        self.assertIsInstance(events_list, list)
        self.assertIsInstance(offset, dict)

    def test_multiple_topics(self):
        """Test reading from multiple different topics."""
        topics_to_test = [
            "/data/AccountChangeEvent",
            "/data/ContactChangeEvent",
        ]

        config = {k: v for k, v in self.config.items() if k != "topic"}

        for topic in topics_to_test:
            with self.subTest(topic=topic):
                connector = LakeflowConnect(config)
                table_name = connector._topic_to_table_name(topic)

                # Test that we can get schema and metadata
                schema = connector.get_table_schema(table_name, {"topic": topic})
                self.assertIsNotNone(schema)

                metadata = connector.read_table_metadata(table_name, {"topic": topic})
                self.assertEqual(metadata["ingestion_type"], "append")


def test_salesforce_pubsub_connector():
    """
    Test the Salesforce Pub/Sub connector using the shared LakeflowConnect test suite.

    This function follows the GitHub connector pattern for running tests with
    the LakeflowConnectTester framework.
    """
    # Inject the Salesforce PubSub LakeflowConnect class into the shared test_suite namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Check for credentials
    has_oauth = config.get("clientId") and config.get("clientSecret")
    has_password = config.get("username") and config.get("password")

    if not has_oauth and not has_password:
        print(
            "Skipping LakeflowConnectTester: No Salesforce credentials configured"
        )
        print(
            "Add clientId and clientSecret to configs/dev_config.json to run integration tests"
        )
        return

    # Ensure topic is configured for list_tables to return results
    if not config.get("topic"):
        config["topic"] = "/data/AccountChangeEvent"

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(config, table_config)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )


if __name__ == "__main__":
    # Run unit tests first
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add unit tests
    suite.addTests(loader.loadTestsFromTestCase(TestSalesforcePubSubConnectorUnit))
    suite.addTests(
        loader.loadTestsFromTestCase(TestSalesforcePubSubConnectorIntegration)
    )

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Then run the LakeflowConnectTester if credentials are available
    print("\n" + "=" * 60)
    print("Running LakeflowConnectTester Suite")
    print("=" * 60)
    test_salesforce_pubsub_connector()
