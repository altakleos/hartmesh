# Reporting preferences

Use `report.py preferences <action> <path>`. These commands need no input export.
`read` returns preferences, revision and SHA-256; missing returns revision
`missing` without creating a file. `validate` requires an existing valid file.

Version 1 fields: `brand` (primary/secondary six-digit hex colours), `exclusions`
(up to64 role/column+equals objects or column=value strings), `summary_length`
(short/standard), `comparisons` (previous_period, same_period_last_year), `charts`
(up to32 identifier names), `currency` (three uppercase letters). Unknown fields,
versions, duplicate keys, malformed UTF-8/JSON, excessive nesting and documents
over64 KiB are rejected as a whole. Valid legacy v1 files are read unchanged.

```bash
SKILL_DIR="<Directory>"
python "${SKILL_DIR:?}/scripts/report.py" preferences read /mnt/user-data/outputs/reports/preferences.json
python "${SKILL_DIR:?}/scripts/report.py" preferences save /mnt/user-data/outputs/reports/preferences.json --from /tmp/requested-preferences.json --expected missing --operation save-choice-1
python "${SKILL_DIR:?}/scripts/report.py" preferences patch /mnt/user-data/outputs/reports/preferences.json --from /tmp/preference-patch.json --expected <returned-revision> --operation change-choice-1
python "${SKILL_DIR:?}/scripts/report.py" preferences reset /mnt/user-data/outputs/reports/preferences.json --expected <returned-revision> --operation reset-choice-1
```

Save takes a complete v1 object. Patch replaces top-level fields, replaces arrays
and objects as a whole, and removes a field whose patch value is null. Reset
retains an empty v1 document with a new revision. None deletes report history.

The first managed mutation adds `_mutation` containing a distinct revision,
operation ID and exact-request SHA-256. Format version stays1. This consumer
metadata is not authenticated identity. Every accepted mutation, including a
same-value save or reset, has a new revision. Use the exact read revision and a
new operation ID for a new intent. Keep both and the exact request for retries.
A matching retry confirms the current document; changed intent or intervening
mutation returns a conflict. A failure after replacement is **outcome uncertain**:
retry the exact intent to reconcile, never blindly issue a new mutation. Conflicts
require reading and resolving the current state before applying another intent.

All cooperating writers use the stable adjacent `.lock` file. Do not delete it.
Atomic replacement and flush protect one document; arbitrary native edits and
external deletion are outside this cooperative contract. Host file editing still
requires existing qualified containment/write-window permissions.

For each build, defaults → saved preferences → temporary override → CLI flags.
A temporary override is a plain validated v1 object, never a saved mutation.
`preferences-used.json` in the retained draft records the saved snapshot/revision,
effective preferences and explicit CLI choices. Report and passive-view schemas
remain unchanged. It records data used, not proof of authenticated authorship.
