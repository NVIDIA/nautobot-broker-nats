# Nautobot Changelog Reporting Through  Nats

## Introduction
This is an [event broker](https://docs.nautobot.com/projects/core/en/next/user-guide/platform-functionality/events/) for [Nautobot](https://github.com/nautobot/nautobot) that publishes events to [NATS](https://nats.io).

## Configuration

Add this to your nautobot_config.py:
```
connect = {}

# Optional path to a credentials file.
if "NATS_CRED" in os.environ:
    connect["user_credentials"] = os.environ["NATS_CRED"]

from nautobot.core.events import register_event_broker
from nautobot_nats_broker import NATSEventBroker

register_event_broker(
    NATSEventBroker(
        servers="nats-server-url",
        stream="nautobot",
        **connect,
    )
)
```

## Example Outputs

This is what shows up on NATS when you have this plugin configured and then create a new Namespace resource:
```
{
  "request": {
    "id": "501a0004-0fdd-404e-b46c-d2a189d868df",
    "user": "test_user"
  },
  "event": "create",
  "model": "ipam.namespace",
  "record": {
    "id": "c310b8cc-bceb-49a2-b323-c109ed828b10",
    "url": "/api/ipam/namespaces/c310b8cc-bceb-49a2-b323-c109ed828b10/",
    "name": "test_namespace",
    "tags": [],
    "created": "2026-03-25T19:18:15.841125Z",
    "display": "test_namespace",
    "location": null,
    "notes_url": "/api/ipam/namespaces/c310b8cc-bceb-49a2-b323-c109ed828b10/notes/",
    "description": "",
    "object_type": "ipam.namespace",
    "last_updated": "2026-03-25T19:18:15.841138Z",
    "natural_slug": "test_namespace_c310",
    "custom_fields": {}
  },
  "@url": "/api/ipam/namespaces/c310b8cc-bceb-49a2-b323-c109ed828b10/",
  "@timestamp": "2026-03-25T19:18:15Z",
  "response": {
    "host": "test_host.example.com"
  }
}

```
