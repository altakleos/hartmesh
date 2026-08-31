# Security Policy

## Supported Versions

As deer-flow doesn't provide an official release yet, please use the latest version to receive security updates.
Currently, we have two branches to maintain:
* main branch for deer-flow 2.x
* main-1.x branch for deer-flow 1.x

## Reporting a Vulnerability

Please go to https://github.com/bytedance/deer-flow/security to report the vulnerability you find.

## Deployment Tenant Boundary

Tenant identity is selected by the operator at service startup. It cannot be selected by an API caller and does not replace per-user authorization.

The Gateway accepts one tenant per process/release. Request headers, bodies,
channel payloads, Scheduled Task context, internal callers, extensions, users,
threads, Helm release names, and Kubernetes namespaces are not authoritative
tenant inputs. Durable production requires an explicit nonlocal identity and a
database/schema bound to its pseudonymous digest. Separate tenant releases must
use separate databases or PostgreSQL schemas; the tenant columns do not provide
general shared-schema row-level isolation.

The canonical tenant identifier is operator configuration. Persisted records,
extensions, health, lifecycle APIs, and support bundles expose only its
pseudonymous reference and digest. That projection is not a secret and may be
guessable for a small identifier space. Redis principals should be restricted
with both key/stream and pub/sub channel ACL patterns derived from the same
identity. See [the deployment and migration guide](backend/docs/TENANT_IDENTITY.md).

## Durable MCP Task Lineage

Durable MCP task lineage contains only server-owned correlation evidence: the
tenant reference/digest, a pseudonymous principal reference, accepted parent
run/task/receipt and assembly anchors when the task came from an Agent tool, safe
server/tool names, and SHA-256 commitments to a structural request projection and
configured credential selector. Standalone API tasks have no parent fields, and
client-supplied lineage, tenant, principal, receipt, revision, or credential
selector fields are ignored.

The credential selector commits to the tenant, principal reference, server,
operator-controlled binding identity, and binding version. Only the resulting
digest and non-sensitive numeric version are persisted. Tokens, API keys, OAuth grants/scopes, secret names, headers,
environment-variable names, encrypted values, refresh state, and failure text are
excluded. MCP argument values are excluded unless the server-owned tool schema
explicitly classifies a bounded scalar as evidence-safe; all other arguments
contribute only field/type/shape or secret-handle markers. Raw results remain in
the existing task result store and never enter lineage or notification Origin.

Task, polling, cancellation, and notification logs use stable codes plus bounded
local IDs or correlation references. They do not log arguments, results, remote
task handles, raw principal identities, credentials, provider messages, exception
messages, or tracebacks. A credential-selector mismatch after restart fails with
`mcp_task_credential_binding_unavailable` rather than silently selecting another
identity under the old lineage.

The first cancellation request stores its tenant-scoped pseudonymous actor
reference and a host-selected `user_api` or `agent_tool` reason separately from
immutable submission lineage. Remote snapshot error strings are always replaced
with fixed host codes, even when a provider prefixes them with `mcp_task_`.
After a lineage-v2 task is written, migration downgrade is blocked so a
pre-lineage binary cannot start against and mutate the newer row shape. A safe
pre-v2 downgrade restores that binary's original remote-task uniqueness rule.

MCP task lineage records who submitted a task and how its completion was correlated. It does not guarantee exactly-once execution by the remote MCP server.
