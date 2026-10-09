# Human-input requests and Attention

**Attention** gives human collaborators a durable inbox for questions attached
to an AI employee's Work. A manager opens Work and selects **Request human input**.
The question, reason and expected response are shared with current Work inspectors.
HartMesh handles attribution, current access, routing and history. Domain rules
and specialist validation remain in skills and optional trusted plugins.

Authenticated humans can create and manage requests. During explicit Work
execution, the AI employee can ask for input and assess the exact set of factual
replies, or revise an inadequate factual question. Human decisions and outcome
acceptance remain authenticated human actions.
Saving a response never starts or resumes a run. An inbox entry proves availability
in the application, not that a person noticed it. No external notification,
email, reminder or background activation is implied.

## Discovery and permissions

Attention has **Needs your input**, **Needs routing**, **Awaiting check** and
**Shared requests** views. Counts reflect current access and action eligibility
before pagination. The navigation refreshes every 30 seconds while mounted;
account changes retire queries and in-flight actions. Failed access revalidation
hides protected cached details and counts.

Each request records one primary human recipient. Creation defaults to the
eligible human supervisor; no eligible default leaves Needs routing. Managers
may explicitly select or clear a recipient. Addressing grants no access and does
not exclude another authorized human from helping. Supervisor changes do not
retarget existing requests. If a recipient loses eligibility, its personal action
count disappears and managers see Needs routing. Legitimately restoring the same
identity's grants restores its existing addressed action; no grant is created.

Inspect permits shared reads; Use plus Inspect permits factual responses.
Manage plus Inspect permits routing, withdrawal and canonical Work decisions or
reviews. Provider `agents:read` and `agents:write` are additional ceilings.
Archived/deleted AI employees cannot receive factual responses. Authorized
history and deliberate management remain available. PATs and caller-selected
human/nonhuman identities are not accepted.

## Responses, decisions and files

An information response becomes **Awaiting check**, preserving the blocker until
qualified assessment or an explicit manager scope change. A later “cannot provide”
reply does not erase already supplied information. Decision/review requests stay
Pending after context, file references or free text such as “approved”. Only the
exact authorized Work decision or outcome acceptance closes them as resolved.
Reading is independent and never resolves a request.

There is one live request per Work, matching its single current blocker or
submitted outcome. Scope changes supersede the request; cancellation withdraws it.
An information/decision request may be explicitly withdrawn with a reason, which
clears that blocker without claiming the information was assessed. A submitted
review requires the existing Work Changes requested/Cancel/Accept controls.
History retains prior responses and routing actions.

References use existing Space locators with current shared-audience admission.
References do not upload, copy, verify contents or mount a resource for the AI
employee. Its native environment currently mounts Home only. Use existing Home
storage controls for a transfer; managers must establish the qualified exclusive
write window after native attachments are fenced. A finished or blocked run is
not that proof. Upload and response are separate: preserve an uploaded file if
response persistence is uncertain, and retry the exact response operation.
Binding decisions recheck READ on their original and response-attached sources.
Outcome acceptance remains explicitly statement-only, with current file contents
unchecked.

## API and limits

| Method/path | Operation |
| --- | --- |
| `POST /api/agent-instances/{instance}/work/{work}/requests` | Create a request against expected Work/assignment revisions |
| `GET /api/human-input` | `view=pending\|routing\|answered\|all`, optional Work/instance filter, counts and page |
| `GET /api/human-input/{id}` | Current authorized request and available controls |
| `GET /api/human-input/{id}/responses` | Attributed responses, oldest first |
| `GET /api/human-input/{id}/history` | Request operations, newest first |
| `POST /api/human-input/{id}/responses` | Text/choice/reference with exact request/assignment revision |
| `POST /api/human-input/{id}/commands` | Route or withdraw with current row/request revision |
| `POST /api/human-input/{id}/read` | Mark a seen row revision read, without changing content revisions |

Request purpose is `information`, `decision` or `review`. Review creation requires
an actual canonical Submitted outcome; clients cannot manufacture one. State is
`pending`, `answered` (information only), or `closed` with a reason of `resolved`,
`superseded` or `withdrawn`.

Question, reason, expected response, response text and each choice are limited
to 4096 characters; there are at most 16 choices and 16 references per command,
200 responses per request, and 100 records per history/list page. Relative file
paths are at most 1024 characters. Pagination offsets are bounded to 100000.
Revisions are positive 31-bit integers and operation IDs are 32-character UUID hex.

Responses append against request/source revisions, not the row revision, so two
independent same-question responses both survive. A routing change advances the
request revision and rejects stale forms. Every write has an exact actor-bound
receipt; retry the same operation ID and body after uncertain persistence.
Canonical Work `decide`/`accept` commands with a live request require
`request_basis: {id, revision, request_revision, response_ids}` naming every
currently assessed response. Legacy `input` must use the linked request instead.
Work edits/cancellation and request closure commit in the same SQL transaction.
No filesystem operation or run launch occurs in that transaction.

## Optional plugin facade

Trusted `PluginContribution(api_version=5, human_input_api_version=1)` backend
actions receive `ActionContext.human_input`, a dependency-free
`deerflow_extension_api.human_input.HumanInputActions` interface. It is bound to
the authenticated human and the current action lifetime. Older plugins retain
their existing context; unsupported hosts reject v5. Model tools receive no
human-input handle, and passive artifact presentations remain non-executable.

`await context.human_input.call(operation, payload)` accepts `list`, `get`,
`responses`, `history`, `create`, `respond`, `command` or `work_command`. Payload
contains the API path/query arguments and, for writes, the canonical JSON command
under `body`. Actor identities, authority objects and unknown arguments are
rejected. Each call rechecks the installed/enabled plugin, action authorization,
human identity, provider ceilings and source grants. Retained handles stop working
when the action exits. Missing capability raises `NotImplementedError`; it never
means an empty successful result. Browser pages call their existing authenticated
backend actions and need no second module loader or lifecycle.

Generic controls remain usable without a plugin. Missing specialist validation
stays unavailable; it never counts as acceptance.

## Schema change

Additive migration `0039_human_input` follows `0038_agent_work`. It adds
`human_input_requests`, `human_input_responses`, `human_input_events` and
`human_input_reads`. No user/chat cascade can erase used requests; downgrade
refuses used data. Existing Work tables, adopted definition hashes, consumer
file formats and runtime configuration versions are unchanged.

Migration `0040_work_execution` adds explicit human/nonhuman attribution to request
creation and history events. Existing records default to human attribution.
Replies remain human-attributed. **Open Work** selects the exact source assignment;
**Resume** is a separate authorized action after a reply is saved.
