# Introduction
This is an [event broker](https://docs.nautobot.com/projects/core/en/next/user-guide/platform-functionality/events/) for [Nautobot](https://github.com/nautobot/nautobot) that publishes events to [NATS](https://nats.io).

# Configuration

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
