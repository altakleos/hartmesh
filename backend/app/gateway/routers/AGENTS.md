# Gateway routers

`POST /api/threads/{id}/history` hydrates reopened chats with the rendered
`artifacts`, `todos`, and non-null `goal` channels in the newest returned
checkpoint alongside title and messages.
Keep that projection explicit and serialized through the API channel serializer;
do not expose internal sandbox state. Empty lists are meaningful, while absent
or null goals are omitted to preserve the frontend's local override semantics.

Files and Shared must stream from one safely opened descriptor, including MIME
sniffing, response metadata and range reads. Preflight paths only determine HTTP
errors; they must never become permission to reopen a path through symlinked
parents. Use `deerflow.files.store.open_regular_source` for source reads/copies,
and retain ownership until every offloaded operation has drained on cancellation.
Conversation keep/publish sources remain lexical paths within uploads/outputs.
