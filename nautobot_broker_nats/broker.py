#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0


import dictdiffer
import orjson
import re
import socket
import time

from django.db.models.signals import post_delete, post_save

from nautobot.core.events import EventBroker

from .client import NATS


class NATSEventBroker(EventBroker):
    def __init__(self, *args, **kwargs):
        # Ignore uninteresting or sensitive topics.
        self.exclude_topics = [
            "nautobot.admin.*",
            "nautobot.jobs.*",
            "nautobot.users.*",
        ]

        # Include everything else.
        self.include_topics = ["*"]

        # Create the underlying NATS client.
        self.client = NATS(**kwargs)

        # The hostname is static and included in every message.
        self.hostname = socket.gethostname()

        # Compile the pattern to extract the event and model from the topic.
        self.pattern = re.compile(r"^nautobot\.(create|update|delete)\.(.+)$")

        # Unfortunately relationship association changes are not published, so
        # we must listen to signals as a workaround until this issue is
        # resolved: https://github.com/nautobot/nautobot/issues/6811
        post_delete.connect(self.signal_delete)
        post_save.connect(self.signal_create)

    # Return the difference between two records represented as dictionaries.
    def diff(self, a: dict, b: dict) -> dict:
        detail = {}

        for diff in dictdiffer.diff(a, b, expand=True):
            field = diff[1]

            # Array change.
            if isinstance(field, list):
                field = field[0]

            if not field:
                continue

            detail[field] = [
                dictdiffer.dot_lookup(a, field),
                dictdiffer.dot_lookup(b, field),
            ]

        return detail

    # Return a message, merged with the contents of data.
    def message(self, data: dict) -> dict:
        # Although the payload has a timestamp, it is not RFC3339 compliant.
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        base = {
            "@timestamp": timestamp,
            "response": {
                "host": self.hostname,
            },
        }

        return {**data, **base}

    # Publish is called by the event broker system when an event occurs.
    def publish(self, *, topic: str, payload: str) -> None:
        match = self.pattern.match(topic)

        # Is this not a topic we're interested in?
        if not match:
            return

        event = match.group(1)
        model = match.group(2)

        # The payload is serialized JSON, and will no longer be necessary when
        # this is resolved: https://github.com/nautobot/nautobot/issues/6807
        data = orjson.loads(payload)

        # Retrieve the record.
        if event == "create" or event == "update":
            record = data["postchange"]
        else:
            record = data["prechange"]

        message = {
            "request": {
                "id": data["context"]["request_id"],
                "user": data["context"]["user_name"],
            },
            "event": event,
            "model": model,
            "record": record,
        }

        # Although the payload contains the set of differences, it isn't
        # compatible with existing change message consumers.
        if event == "update":
            message["detail"] = self.diff(
                data["prechange"],
                data["postchange"],
            )

        if "url" in record:
            message["@url"] = record["url"]

        self.client.publish(self.message(message))

    def signal(self, event, instance, **kwargs) -> None:
        # Construct the full model name.
        model = instance._meta.app_label + "." + instance._meta.model_name

        # A few models are not published by the event framework, so must be
        # handled by the signal handler.
        if model not in {"extras.relationshipassociation", "ipam.ipaddresstointerface"}:
            return

        # This is deferred to avoid an "Apps aren't loaded yet" exception.
        from nautobot.core.models.utils import serialize_object_v2

        self.client.publish(
            self.message(
                {
                    "event": event,
                    "model": model,
                    "record": serialize_object_v2(instance),
                }
            )
        )

    def signal_create(self, instance, **kwargs):
        self.signal("create", instance, **kwargs)

    def signal_delete(self, instance, **kwargs):
        self.signal("delete", instance, **kwargs)
