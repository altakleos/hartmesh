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
