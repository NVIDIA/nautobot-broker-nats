#  SPDX-FileCopyrightText: Copyright (c) "2025" NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#  SPDX-License-Identifier: APACHE 2.0
"""This plugin formats and publishes changelog events to a NATs queue."""

import typing

# Importing broker eagerly pulls in nautobot.core.events, which requires
# configured Django settings. Keep client-only consumers and tests importable.
if typing.TYPE_CHECKING:
    from .broker import NATSEventBroker as NATSEventBroker

__all__ = ["NATSEventBroker"]


def __getattr__(name: str) -> typing.Any:
    """Load the Nautobot-dependent broker only when it is requested."""
    if name == "NATSEventBroker":
        from .broker import NATSEventBroker

        return NATSEventBroker
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
