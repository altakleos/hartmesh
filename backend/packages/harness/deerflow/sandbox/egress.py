"""Run-bound egress for the accepted session Kind.

An ordinary session asks a person about a blocked destination and the grant
lives in that container's sidecar. The accepted Kind cannot take that path: a
container-bound grant would outlive nothing (retire destroys the container)
but would also record nothing about the run that was held to it. Instead the
accepted Kind declares its egress at admission, the way it declares its
execution budget:

* the operator's ``execution_policy.accepted_egress`` is the ceiling;
* a caller may only narrow it (``context.egress_allowance``);
* the canonical :class:`EgressAllowanceV1` and its digest are part of the
  accepted invocation's runtime identity;
* the Kind's Material renders it into the container's network policy, and the
  provisioner attests the digest it rendered.

Recovery re-provisions the same allowance because it is read from the
accepted row, never from a container. Nothing mid-run can widen it.

Rules are CIDR based because that is what a Kubernetes ``NetworkPolicy`` can
express; names are resolved by the sandbox only when ``dns`` is allowed. The
:data:`NEVER_ALLOWED_NETWORKS` set (private, loopback, link-local, carrier NAT,
multicast, documentation, and cloud-metadata ranges) can never be allowed, and
a wide rule is rendered with those ranges carved out. The provisioner keeps an
identical copy of that set; ``tests/test_provisioner_egress.py`` pins the two.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Self

EGRESS_ALLOWANCE_VERSION = 1
MAX_EGRESS_RULES = 64
DEFAULT_EGRESS_PROFILE = "accepted-egress-v1"
DENY_ALL_EGRESS_PROFILE = "deny-all-v1"

_DOMAIN = b"hartmesh.egress-allowance/v1\0"
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_PROFILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_PROTOCOLS = ("TCP", "UDP")

# Kept byte-for-byte identical to the provisioner's copy; see the module docstring.
NEVER_ALLOWED_NETWORKS: tuple[str, ...] = (
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "::ffff:0:0/96",
    "64:ff9b:1::/48",
    "100::/64",
    "2001:db8::/32",
    "fc00::/7",
    "fe80::/10",
    "ff00::/8",
)
_NEVER_ALLOWED = tuple(ipaddress.ip_network(value) for value in NEVER_ALLOWED_NETWORKS)


class EgressPolicyError(ValueError):
    """Stable, non-sensitive reason codes for egress allowance failures."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def egress_allowance_digest(projection: Mapping[str, object]) -> str:
    """Digest the digest-free projection; the provisioner recomputes this exactly."""

    return hashlib.sha256(_DOMAIN + _canonical_bytes(projection)).hexdigest()


def _require_digest(value: object) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise EgressPolicyError("egress_allowance_digest_invalid")
    return value


def _parse_network(cidr: object) -> ipaddress.IPv4Network | ipaddress.IPv6Network:
    if not isinstance(cidr, str) or not cidr or len(cidr) > 64 or "/" not in cidr:
        raise EgressPolicyError("egress_rule_cidr_invalid")
    try:
        return ipaddress.ip_network(cidr, strict=True)
    except ValueError as exc:
        raise EgressPolicyError("egress_rule_cidr_invalid") from exc


def _is_public(network: ipaddress.IPv4Network | ipaddress.IPv6Network) -> bool:
    return not any(network.version == never.version and network.subnet_of(never) for never in _NEVER_ALLOWED)


@dataclass(frozen=True, slots=True)
class EgressRuleV1:
    """One public destination network, one protocol, one port or every port."""

    cidr: str
    protocol: Literal["TCP", "UDP"]
    port: int | None

    def __post_init__(self) -> None:
        network = _parse_network(self.cidr)
        if network.compressed != self.cidr:
            raise EgressPolicyError("egress_rule_cidr_invalid")
        if not _is_public(network):
            raise EgressPolicyError("egress_rule_not_public")
        if self.protocol not in _PROTOCOLS:
            raise EgressPolicyError("egress_rule_protocol_invalid")
        if self.port is not None and (type(self.port) is not int or not 1 <= self.port <= 65535):
            raise EgressPolicyError("egress_rule_port_invalid")

    @classmethod
    def build(cls, *, cidr: object, protocol: object = "TCP", port: object = None) -> Self:
        network = _parse_network(cidr)
        if not isinstance(protocol, str) or protocol.upper() not in _PROTOCOLS:
            raise EgressPolicyError("egress_rule_protocol_invalid")
        return cls(cidr=network.compressed, protocol=protocol.upper(), port=port)  # type: ignore[arg-type]

    @classmethod
    def from_json(cls, value: object) -> Self:
        if not isinstance(value, Mapping) or set(value) - {"cidr", "protocol", "port"}:
            raise EgressPolicyError("egress_allowance_field_invalid")
        return cls.build(cidr=value.get("cidr"), protocol=value.get("protocol", "TCP"), port=value.get("port"))

    @property
    def network(self) -> ipaddress.IPv4Network | ipaddress.IPv6Network:
        return ipaddress.ip_network(self.cidr)

    @property
    def sort_key(self) -> tuple[int, int, int, str, int]:
        network = self.network
        return (network.version, int(network.network_address), network.prefixlen, self.protocol, -1 if self.port is None else self.port)

    def covers(self, other: EgressRuleV1) -> bool:
        """Whether ``other`` reaches nothing this rule does not already allow."""

        mine, theirs = self.network, other.network
        return self.protocol == other.protocol and mine.version == theirs.version and theirs.subnet_of(mine) and (self.port is None or self.port == other.port)

    def to_json(self) -> dict[str, object]:
        return {"cidr": self.cidr, "protocol": self.protocol, "port": self.port}


@dataclass(frozen=True, slots=True)
class EgressAllowanceV1:
    """Canonical immutable egress declared for one accepted invocation."""

    version: Literal[1]
    profile: str
    dns: bool
    rules: tuple[EgressRuleV1, ...]
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != EGRESS_ALLOWANCE_VERSION:
            raise EgressPolicyError("egress_allowance_version_unsupported")
        if not isinstance(self.profile, str) or _PROFILE_RE.fullmatch(self.profile) is None:
            raise EgressPolicyError("egress_allowance_profile_invalid")
        if type(self.dns) is not bool:
            raise EgressPolicyError("egress_allowance_field_invalid")
        if not isinstance(self.rules, tuple) or any(not isinstance(rule, EgressRuleV1) for rule in self.rules):
            raise EgressPolicyError("egress_allowance_field_invalid")
        if len(self.rules) > MAX_EGRESS_RULES:
            raise EgressPolicyError("egress_allowance_rule_limit")
        keys = [rule.sort_key for rule in self.rules]
        if keys != sorted(keys) or len(set(keys)) != len(keys):
            raise EgressPolicyError("egress_allowance_not_canonical")
        if _require_digest(self.digest) != egress_allowance_digest(self._projection()):
            raise EgressPolicyError("egress_allowance_digest_invalid")

    def _projection(self) -> dict[str, object]:
        return {
            "version": self.version,
            "profile": self.profile,
            "dns": self.dns,
            "rules": [rule.to_json() for rule in self.rules],
        }

    @classmethod
    def build(cls, *, profile: str, dns: bool, rules: Sequence[EgressRuleV1]) -> Self:
        if not isinstance(rules, Sequence) or isinstance(rules, str | bytes):
            raise EgressPolicyError("egress_allowance_field_invalid")
        canonical = tuple(sorted({rule.sort_key: rule for rule in rules}.values(), key=lambda rule: rule.sort_key))
        projection = {"version": EGRESS_ALLOWANCE_VERSION, "profile": profile, "dns": dns, "rules": [rule.to_json() for rule in canonical]}
        if type(dns) is not bool:
            raise EgressPolicyError("egress_allowance_field_invalid")
        return cls(version=EGRESS_ALLOWANCE_VERSION, profile=profile, dns=dns, rules=canonical, digest=egress_allowance_digest(projection))

    @classmethod
    def deny_all(cls, *, profile: str = DENY_ALL_EGRESS_PROFILE) -> Self:
        """The accepted Kind's default: no destination, no name resolution."""

        return cls.build(profile=profile, dns=False, rules=())

    def to_json(self) -> dict[str, object]:
        return {**self._projection(), "digest": self.digest}

    @classmethod
    def from_json(cls, value: object) -> Self:
        if not isinstance(value, Mapping) or set(value) != {"version", "profile", "dns", "rules", "digest"} or value.get("version") != EGRESS_ALLOWANCE_VERSION:
            raise EgressPolicyError("egress_allowance_version_unsupported")
        raw_rules = value["rules"]
        if not isinstance(raw_rules, Sequence) or isinstance(raw_rules, str | bytes):
            raise EgressPolicyError("egress_allowance_field_invalid")
        rules = tuple(EgressRuleV1.from_json(entry) for entry in raw_rules)
        return cls(version=EGRESS_ALLOWANCE_VERSION, profile=value["profile"], dns=value["dns"], rules=rules, digest=value["digest"])  # type: ignore[arg-type]

    def narrow(self, requested: Mapping[str, object]) -> EgressAllowanceV1:
        """Apply a caller's request; anything this allowance does not cover is refused."""

        if not isinstance(requested, Mapping):
            raise EgressPolicyError("egress_allowance_request_invalid")
        forbidden = set(requested) - {"dns", "rules"}
        if forbidden:
            raise EgressPolicyError("egress_allowance_field_forbidden")
        dns = self.dns
        if "dns" in requested:
            candidate = requested["dns"]
            if type(candidate) is not bool:
                raise EgressPolicyError("egress_allowance_field_invalid")
            if candidate and not self.dns:
                raise EgressPolicyError("egress_allowance_broadening_forbidden")
            dns = candidate
        rules: tuple[EgressRuleV1, ...] = self.rules
        if "rules" in requested:
            raw_rules = requested["rules"]
            if not isinstance(raw_rules, Sequence) or isinstance(raw_rules, str | bytes):
                raise EgressPolicyError("egress_allowance_field_invalid")
            if len(raw_rules) > MAX_EGRESS_RULES:
                raise EgressPolicyError("egress_allowance_rule_limit")
            narrowed = []
            for entry in raw_rules:
                rule = EgressRuleV1.from_json(entry)
                if not any(ceiling.covers(rule) for ceiling in self.rules):
                    raise EgressPolicyError("egress_allowance_broadening_forbidden")
                narrowed.append(rule)
            rules = tuple(narrowed)
        return EgressAllowanceV1.build(profile=self.profile, dns=dns, rules=rules)


def resolve_egress_allowance(config: Any, *, requested: Mapping[str, object] | None = None) -> EgressAllowanceV1:
    """Resolve the operator ceiling, then the optional caller narrowing."""

    if requested is not None and not isinstance(requested, Mapping):
        raise EgressPolicyError("egress_allowance_request_invalid")
    rules = []
    for entry in getattr(config, "allow", ()) or ():
        rules.append(
            EgressRuleV1.build(
                cidr=getattr(entry, "cidr", None),
                protocol=getattr(entry, "protocol", "TCP"),
                port=getattr(entry, "port", None),
            )
        )
    ceiling = EgressAllowanceV1.build(
        profile=str(getattr(config, "profile", DEFAULT_EGRESS_PROFILE)),
        dns=bool(getattr(config, "dns", False)),
        rules=rules,
    )
    return ceiling if requested is None else ceiling.narrow(requested)


__all__ = [
    "DEFAULT_EGRESS_PROFILE",
    "DENY_ALL_EGRESS_PROFILE",
    "EGRESS_ALLOWANCE_VERSION",
    "MAX_EGRESS_RULES",
    "NEVER_ALLOWED_NETWORKS",
    "EgressAllowanceV1",
    "EgressPolicyError",
    "EgressRuleV1",
    "egress_allowance_digest",
    "resolve_egress_allowance",
]
