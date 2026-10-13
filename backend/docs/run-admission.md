# Gateway run admission

## Managed personal chat capacity

Managed ordinary chats check the current authenticated storage projection before checkpoint history seeding and durable run admission. A first-conversation capacity refusal is HTTP 507, before any streaming response; absent User storage still permits mountless text chat. Instance runs use their own storage policy and never consult the requesting human's personal capacity.

Recorded retries are read by indexed idempotency key before capacity checks, validating thread and owner and applying the existing request comparison. A capacity refusal repeats the lookup to recover a run concurrently admitted by another worker. Peer handles stay unregistered; a local admission that overlaps store I/O retains its canonical task and stream identity. Worker capacity checks remain necessary for changes after preflight. No filesystem I/O belongs under the run-manager lock, and no await may separate a new durable admission from worker attachment.

Run stores may omit indexed key lookup and retain their existing unkeyed
admission behavior. Such a lookup raises `NotImplementedError`; it is never
treated as a missing record. Memory and SQL stores support managed keyed retries.

## Durable MCP notifications

Durable MCP notifications use internal Agent runs. Keep their trusted delivery instruction outside the user-input boundary, and frame serialized remote events as untrusted before model invocation. Strict thread existence/ownership admission dead-letters events whose task outlives its deleted chat instead of recreating the thread. `start_run` marks run-manager admission conflicts with `BusyThreadConflict`; the notification launcher retries only that typed 409 and treats other 409 responses as permanent.
