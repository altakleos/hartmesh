"""Company connection grants belong to an instance, independently of its adopted brief."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import urlsplit

from deerflow.agent_instances.contract import AgentConflict
from deerflow.capabilities.runtime import installation_id
from deerflow.config.extensions_config import (
    ExtensionsConfig,
    McpServerConfig,
    atomic_write_extensions_config,
    extensions_config_file_lock,
    extensions_config_write_lock,
    read_raw_extensions_config,
    validate_raw_extensions_config,
)

BINDING_SECRET = "instance_binding_secret"


def _conflict(reason):
    return AgentConflict("Company connection is unavailable.", reason=reason)


def binding_id(definition):
    metadata = definition.get("capability")
    secret = metadata.get(BINDING_SECRET) if isinstance(metadata, dict) else None
    if isinstance(secret, str) and re.fullmatch("(?:adopted:)?[0-9a-f]{64}", secret):
        return hashlib.sha256(secret.encode("ascii")).hexdigest()
    return None


def preserve_registry_bindings(previous_servers, servers):
    """Preserve the registry-owned field on edits. Discard a client-supplied value on create."""
    if not isinstance(previous_servers, dict) or not isinstance(servers, dict):
        raise ValueError("The MCP server registry must be an object")
    for name, candidate in servers.items():
        if not isinstance(candidate, dict):
            continue  # The configuration validator diagnoses the definition.
        previous = previous_servers.get(name)
        previous_meta = previous.get("capability") if isinstance(previous, dict) else None
        target = candidate.get("capability")
        if isinstance(target, dict):
            target.pop(BINDING_SECRET, None)
        if isinstance(previous_meta, dict):
            # A client may omit the host's catalog identity or binding on edit.
            keep = {k: previous_meta[k] for k in ("id", BINDING_SECRET) if k in previous_meta}
            if keep:
                if not isinstance(target, dict):
                    target = dict(previous_meta)
                    candidate["capability"] = target
                target.update(keep)
        elif name not in previous_servers and candidate.get("type", candidate.get("transport")) in {"http", "sse"}:
            target = candidate.setdefault("capability", {})
            if not isinstance(target, dict):
                raise ValueError("Remote connection installation metadata must be an object")
            target[BINDING_SECRET] = secrets.token_hex(32)


def sdk_server_definitions(config):
    servers = {name: server.model_dump() for name, server in config.mcp_servers.items()}
    for definition in servers.values():
        metadata = definition.get("capability")
        if isinstance(metadata, dict):
            metadata.pop(BINDING_SECRET, None)
    return servers


def _reason(server):
    if server.type not in {"http", "sse"}:
        return "not-remote"
    try:
        address = urlsplit(server.url or "")
        if address.scheme not in {"http", "https"} or not address.hostname or address.username is not None or address.password is not None or address.fragment or any(c.isspace() for c in server.url):
            return "not-available"
        _ = address.port
    except ValueError:
        return "not-available"
    if server.user_auth and server.user_auth.enabled:
        return "per-person-credentials"
    if server.headers_from_context and server.headers_from_context.enabled:
        return "per-request-credentials"
    if server.agent_auth:
        return "agent-credential"
    if server.task_toolsets:
        return "long-running"
    if not server.enabled:
        return "switched-off"
    return None


def _known_grant(value):
    if not isinstance(value, dict) or set(value) != {"given_by", "given_at"}:
        return False
    if not isinstance(value["given_by"], str) or not value["given_by"] or not isinstance(value["given_at"], str):
        return False
    try:
        return datetime.fromisoformat(value["given_at"]).tzinfo is not None
    except ValueError:
        return False


def _object(parent, key, *, create=False):
    if not isinstance(parent, dict):
        return {}
    if key not in parent and create:
        parent[key] = {}
    result = parent.get(key, {})
    if not isinstance(result, dict):
        if create:
            raise _conflict("not-available")
        return {}
    return result


def _grants(raw, instance, *, create=False):
    return _object(_object(_object(raw, "agentConnections", create=create), "agents", create=create), instance, create=create)


@dataclass(frozen=True)
class CompanyConnection:
    name: str
    selector: str
    binding: str | None
    reason: str | None
    server: McpServerConfig = field(repr=False)
    stored: dict = field(repr=False)


def _selector(name, identity, binding):
    return hashlib.sha256(json.dumps([name, identity, binding], ensure_ascii=True).encode()).hexdigest()


def _connections(raw):
    servers = _object(raw, "mcpServers")
    try:
        effective = validate_raw_extensions_config(raw).mcp_servers
    except (ValueError, TypeError):
        raise _conflict("not-available") from None
    identities = {name: installation_id(name, definition) for name, definition in servers.items()}
    counts = Counter(identities.values())
    bindings = {name: binding_id(definition) for name, definition in servers.items()}
    binding_counts = Counter(bindings.values())
    for name, definition in servers.items():
        identity = identities[name]
        binding = bindings[name]
        selector = _selector(name, identity, binding)
        reason = _reason(effective[name])
        metadata = definition.get("capability", {})
        if not isinstance(metadata, dict) or (BINDING_SECRET in metadata and not binding) or ("id" in metadata and (not isinstance(metadata["id"], str) or not metadata["id"].strip())):
            reason = "not-available"
        elif counts[identity] != 1 or (binding and binding_counts[binding] != 1):
            reason = "ambiguous"
        yield CompanyConnection(name, selector, binding, reason, effective[name], definition)


def _current():
    try:
        path = ExtensionsConfig.resolve_config_path()
        return read_raw_extensions_config(path) if path else {}
    except (OSError, ValueError, RuntimeError):
        raise _conflict("not-available") from None


def granted_connections(instance):
    """Read the current file strictly. Only a valid, enabled, uniquely bound grant is usable."""
    raw = _current()
    entries = _grants(raw, instance)
    return [item for item in _connections(raw) if item.binding and not item.reason and _known_grant(entries.get(item.binding))]


def list_connections(instance, *, manager):
    raw = _current()
    entries = _grants(raw, instance)
    result = []
    for item in _connections(raw):
        record = entries.get(item.binding)
        given = item.binding is not None and _known_grant(record)
        if not manager and (not given or item.reason):
            continue
        try:
            host = urlsplit(item.stored.get("url") or "").hostname or ""
        except ValueError:
            host = ""
        view = {"name": item.name, "host": host}
        if manager:
            view.update(
                id=item.selector,
                description=item.stored.get("description") or "",
                given=given,
                given_at=record["given_at"] if given else None,
                given_by=record["given_by"] if given else None,
                can_give=item.reason in {None, "switched-off"},
                reason=item.reason,
            )
        result.append(view)
    return result


def connection_recipients(raw):
    """Project grants for a current connection. The caller filters deleted instances."""
    agents = _object(_object(raw, "agentConnections"), "agents")
    names = {item.binding: item.name for item in _connections(raw) if item.binding and item.reason != "ambiguous"}
    recipients = {}
    for instance_id, entries in agents.items():
        if isinstance(entries, dict):
            for binding, record in entries.items():
                if binding in names and _known_grant(record):
                    recipients.setdefault(names[binding], set()).add(instance_id)
    return recipients


def change_grant(instance, selector, actor_id, *, give):
    """The caller retains current Manage authority through this locked change."""
    try:
        file = ExtensionsConfig.resolve_config_path()
    except (OSError, ValueError, RuntimeError):
        raise _conflict("not-available") from None
    if not file:
        raise _conflict("removed")
    with extensions_config_write_lock, extensions_config_file_lock(file):
        try:
            raw = read_raw_extensions_config(file)
        except (OSError, ValueError, RuntimeError):
            raise _conflict("not-available") from None
        target = None
        for item in _connections(raw):
            if item.selector == selector:
                target = item
                break
            # Concurrent requests may both have selected a legacy row before its
            # first grant. Its adoption receipt is never minted by a create.
            metadata = item.stored.get("capability")
            secret = metadata.get(BINDING_SECRET) if isinstance(metadata, dict) else None
            if item.binding and isinstance(secret, str) and secret.startswith("adopted:") and _selector(item.name, installation_id(item.name, item.stored), None) == selector:
                target = item
                break
        if target is None:
            if not give:
                return
            raise _conflict("removed")
        entries = _grants(raw, instance, create=give)
        key = target.binding
        if give:
            if target.reason not in {None, "switched-off"}:
                raise _conflict(target.reason)
            if key is None:
                metadata = target.stored.setdefault("capability", {})
                if not isinstance(metadata, dict) or BINDING_SECRET in metadata:
                    raise _conflict("not-available")
                metadata[BINDING_SECRET] = "adopted:" + secrets.token_hex(32)
                key = binding_id(target.stored)
            if key in entries:
                if not _known_grant(entries[key]):
                    raise _conflict("not-available")
                return
            entries[key] = {"given_by": actor_id, "given_at": datetime.now(UTC).isoformat()}
        elif key in entries:
            del entries[key]
        else:
            return
        validate_raw_extensions_config(raw)
        atomic_write_extensions_config(file, raw)


def current_grant(instance, binding):
    """Resolve one issued grant from current on-disk state, with a fixed refusal reason."""
    raw = _current()
    if not binding:
        raise _conflict("removed")
    matches = [item for item in _connections(raw) if item.binding == binding]
    if not matches:
        raise _conflict("removed")
    if len(matches) != 1:
        raise _conflict("ambiguous")
    item = matches[0]
    if not _known_grant(_grants(raw, instance).get(binding)):
        raise _conflict("not-given")
    if item.reason is not None:
        raise _conflict(item.reason)
    return item
