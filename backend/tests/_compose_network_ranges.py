"""Address space the shipped Compose ``app`` network must stay clear of.

A Docker bridge is a *connected route* inside whatever host runs it, so every
address in a pinned subnet stops being reachable through that host's real
default gateway. The tenant VM profile's ``app`` network
(``deploy/compose/compose.yaml``) pins one, and its default must clear the
ranges below. Networks an operator runs around the host are site-specific:
the bundle guide tells them to check the guest's own routes before onboarding
and to override ``HARTMESH_APP_SUBNET`` where the default collides.

``RESERVED_RANGES`` holds address space with a standard meaning that a guest
commonly routes: the RFC 6598 shared range used by carrier-grade NAT and
overlay networks.

``DOCKER_DEFAULT_POOLS`` is Docker's own: the ``docker0`` bridge, and the two
address pools every network the file does *not* pin is allocated from -- the
per-sandbox bridges under ``allowlist``, ``hartmesh_sandbox`` under ``open``,
and any unpinned compose network. Staying outside them keeps a pinned network
from colliding with one of those and from spending a whole /16 the daemon would
then skip as overlapping.
"""

from __future__ import annotations

RESERVED_RANGES = ("100.64.0.0/10",)

DOCKER_DEFAULT_POOLS = ("172.17.0.0/16", "172.16.0.0/12", "192.168.0.0/16")
