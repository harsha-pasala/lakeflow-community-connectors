"""
Salesforce Pub/Sub Lakeflow Connector

A self-contained LakeflowConnect implementation for Salesforce Pub/Sub API.
Supports CDC events, Platform Events, and Custom Events.
"""

from .salesforce_pubsub import (
    LakeflowConnect,
    PubSubAPIClient,
    create_client_from_options,
)

__version__ = "1.0.0"
__all__ = [
    "LakeflowConnect",
    "PubSubAPIClient",
    "create_client_from_options",
]
