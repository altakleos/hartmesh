# User persistence

`user_preferences` stores independent `(user_id, key)` rows in the shared SQL
database. PATCH upserts only supplied keys in one transaction; `null` resets a
field, disjoint edits commute, and same-field writes are last-commit-wins.

The Gateway's `GET/PATCH /api/v1/auth/preferences` allows only notification
enablement, the default model, conversation mode, and reasoning effort. It
requires a browser session plus `X-Expected-User-Id` matching that session (a
stale-tab guard, never an authorization source); PAT, internal, and auth-disabled
callers are rejected. Never persist arbitrary agent context or credentials
through this API. See `backend/docs/API.md` for the HTTP contract.

Local credential writes use `rehash_password` / `replace_password`, never a whole-user update. Rehash compares the verified hash and token version without changing other fields. Interactive password changes compare both and return 409 on a stale request; operator resets increment the persisted version unconditionally. SQL `RETURNING` supplies the write's cookie version before commit; do not reload it after commit and adopt a concurrent reset's version. Email casing is preserved on unchanged legacy addresses.
