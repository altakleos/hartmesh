# Optional quote response example

This trusted extension contributes one page at
`/workspace/extensions/example.work-input/quote`. It uses the public human-input
facade v1, negotiated through plugin contract v5, to read a current factual
request and append an authenticated human response. It adds no model tools,
core domain schemas, execution triggers, decisions or approval records.

The form checks bounded supplier text, canonical delivery date, uppercase
currency and a quoted total with two decimal places. These are field-format
checks, not verification of the supplier or commercial facts. The receipt says
that AI employee assessment remains outstanding. Human decisions/reviews use
canonical Attention controls. The request ID selects a record; current source
permissions and provider ceilings remain authoritative on every action.

Deploy only through the existing trusted extension lifecycle. The optional
package lives here as source; it is not installed or enabled by this change.
Its entry point is `deerflow_extension_work_input:install`. Restart is required
after deployment/configuration changes, as with other extensions. There are no
new dependencies beyond the public extension API.

When absent, disabled, incompatible, failing or removed/restarted, canonical
requests/responses/history and generic Attention remain available. Specialized
validation is then unavailable; a plain response does not claim that it ran.
Invalid field responses explicitly confirm no submission, allowing correction.
Transport or canonical mutation failures keep the exact operation ID/body for
retry, and link to the exact Attention request for reconciliation. A successful
reply does not resume an AI employee run. Refreshing/unmounting retires this
page; inspect canonical Attention before making a new intent after a lost reply.

The backend qualification imports this actual package and calls the production
action route/facade on SQLite and PostgreSQL. Browser tests load this actual
module through the host plugin transport with deterministic HTTP fixtures.
Neither test tier proves commercial correctness or actual-model reliability.
