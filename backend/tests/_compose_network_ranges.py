"""Address space the shipped Compose networks must stay clear of.

A Docker bridge is a *connected route* inside whatever host runs it, so every
address in a pinned subnet stops being reachable through that host's real
default gateway. Two shipped files pin one: the tenant VM profile's ``app``
network (``deploy/compose/compose.yaml``) and the development stack's
``deer-flow-dev`` (``docker/docker-compose-dev.yaml``). Both are run inside the
same operator's networks, so both answer to one exclusion list, and it lives
here rather than in either test module so the two cannot drift apart.

``EXTERNAL_RANGES`` is that operator's own address space as recorded on
2026-09-08; the two kosmos /16s (pods, services) are the ones the tenant
profile's ``172.30.10.0/24`` sat inside, and ``192.168.200.0/24`` is the one the
development stack sat on.

``DOCKER_DEFAULT_POOLS`` is Docker's own: the ``docker0`` bridge, and the two
address pools every network these files do *not* pin is allocated from -- the
per-sandbox bridges under ``allowlist``, ``hartmesh_sandbox`` under ``open``,
and any unpinned compose network. Staying outside them keeps a pinned network
from colliding with one of those and from spending a whole /16 the daemon would
then skip as overlapping.
"""

from __future__ import annotations

EXTERNAL_RANGES = (
    "10.17.0.0/16",
    "10.18.10.0/24",
    "10.199.199.248/29",
    "100.64.0.0/10",
    "172.16.220.0/22",
    "172.16.224.0/24",
    "172.30.0.0/16",
    "172.31.0.0/16",
    "192.168.1.0/24",
    "192.168.200.0/24",
)

DOCKER_DEFAULT_POOLS = ("172.17.0.0/16", "172.16.0.0/12", "192.168.0.0/16")
