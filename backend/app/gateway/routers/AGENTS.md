# Gateway routers

`POST /api/threads/{id}/history` hydrates reopened chats with the rendered
`artifacts`, `todos`, and non-null `goal` channels in the newest returned
checkpoint alongside title and messages.
Keep that projection explicit and serialized through the API channel serializer;
do not expose internal sandbox state. Empty lists are meaningful, while absent
or null goals are omitted to preserve the frontend's local override semantics.
