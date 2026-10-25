# Correlated channel execution

This is an inactive generic foundation. Managed Telegram model runs, publication
recovery and final-output completeness are separate stages. A server receipt or
successful run status alone never proves a complete final answer.

## Host capability

`InboundMessage.execution_required` and `execution_context` are host-only values.
Literal `False` plus `None` preserves the ordinary path. A missing required
context, malformed flag or malformed context refuses before default-client
construction. A supplied valid context always uses its capability, including
when a check refuses. Metadata cannot manufacture or replace this capability.

The frozen scope captures publication, real human owner, connection, chat,
original thread, endpoint fingerprint and resolved factory/AI employee target
before asynchronous queueing or preparation. Conflicting carriers refuse. The
manager uses the supplied client without changing the current conversation map
or remembered target. It preserves original content and file metadata.

The generic facade uses a retained HTTP owner through public JSON/SSE methods.
It detaches JSON off-loop, validates the detached snapshot and dispatches only
that snapshot. Immediately before transport it awaits operation authorization,
requires literal current authorization, then checks the owner's dispatch gate.
The creating request carries a stable publication Idempotency-Key, explicit
continue-on-disconnect and null completion. It never retries a creating POST.
Read timeouts are 30 seconds; creating connect/pool timeouts are 5 seconds and
read/write timeouts 300 seconds. Streams use public SDK decoding without reconnect.
Post-response authorization runs inside the same retained owner task or stream
pump. Closing an iterator drains that check; a closed iterator cannot yield a
late part. Delivery also rechecks current read authority after its final await.

Closing run admission blocks new creating dispatch only. A dispatched wait or
stream retains read authorization through manager drain. Preparation borrows the
owner; only final supervisor teardown closes it, before persistence. Network
cancellation stays prompt; started codec, SQL and resource cleanup remain owned
and drained. Errors carry fixed protocol text, never provider payloads or secrets.

## Creating v1

`RunCreateRequest.channel_execution` accepts exactly integer version 1 and a
32-character lowercase hexadecimal publication ID. Its presence requires trusted
internal authentication and a currently available real human owner. The Gateway
requires an existing thread owned by that exact human and an actual configured
`AgentConversations` authority proving no instance binding. Missing authority
returns 503; an instance conversation returns 409 without preparing execution.

Version 1 admits one genuine visible human message. Resume/checkpoint input,
human-input-card responses, hidden/framework-only or ambiguous input, bootstrap,
Work activation, cancel-on-disconnect and non-reject concurrency are unsupported.
These checks precede instance readiness or preparation effects. Ordinary callers
retain their original ownership/validation order and defaults.

Incoming `channel_turn_id` markers are removed even for untyped internal message
input and from request/config metadata. On a detached normalized message, only
the admitted genuine human receives `channel_turn_id=<publication_id>`. Live
run/checkpoint metadata carries the same server stamp. The canonical persisted
input and request hash are computed after stamping; caller objects are unchanged.

Server-owned `runs.kwargs.channel_execution` contains version, publication ID,
raw human owner, thread, canonical factory (`lead_agent`) and the actual loaded
`AgentConfig.name` or null. The private worker carriers are pinned to that actual
definition. The run row's requested assistant alias can differ from its canonical
factory. The receipt is never recovered from request metadata or worker text.

Every idempotent reuse checks protocol presence in both directions, full receipt,
owner/thread, canonical input, requested assistant and disconnect mode. Existing
knowledge-default hash compatibility applies only alongside these checks. Legacy
metadata cannot promote a run to v1. Durable admission and worker attachment have
no intervening await. Only an actual v1 busy admission returns HTTP 409 with
`{"detail":{"code":"thread_busy"}}`; other conflicts and SSE error frames do not
establish busy status. Ordinary busy responses keep their text.

## Strict reads

Exact run, run-page and run-message endpoints opt in with
`X-DeerFlow-Require-Channel-Execution: 1`. Supported completed lookups acknowledge
`X-DeerFlow-Channel-Execution-Version: 1`. Unknown or duplicate request versions
return 422. Missing acknowledgement means uncertainty, including a legacy 404.

The server revalidates the real human and actual stored thread owner, then reads
under that raw human's context. It checks actual run owner/thread and validates
any present receipt before projecting a response. Explicit scope denials return
404 without acknowledgement. A completed supported lookup finding no run returns
404 with acknowledgement. Store, mapper, projection and backend failures return
a fixed 503 without acknowledgement, even if a backend raises HTTP 404 itself.
An already-started SQL source read drains before cancellation leaves its owner
scope and session; teardown must not abandon a database connection.

`RunManager.get`/`aget` and `list_by_thread` use `require_store=True` together with
`raise_on_store_error=True`. Configured MemoryRunStore is valid authority. A cached
positive is process evidence, not a health probe; missing authority is rejected
even with cache. Actual misses and pages query the store. Strict hydration maps
observed rows before post-await cache precedence; pages validate duplicates too.
Malformed raw kwargs cannot become legacy receipt absence. Ordinary compatibility
and existing raise-only lifecycle semantics remain unchanged.

Message pages validate the complete event envelope, owner when present, exact
thread/run, strictly increasing positive sequence, exclusive cursor bounds,
message category, event type, content shape, metadata and timezone-aware timestamp
before acknowledgement. This proves a supported source lookup, not final-output
completeness. The facade also checks the acknowledgement and exact receipt scope;
absence or ambiguity permits further reads, never a second creating POST.

Schema remains revision `0043_telegram_inbox_chat_scope`; config stays version 55.
