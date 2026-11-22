# Message search

This optional Gateway extension indexes visible conversation messages in PostgreSQL.
Install the package and select `hartmesh_message_search:install` in the operator's
plugin configuration with the table prefix `hm_message_search_`. A search starts
a bounded background reader when its admitted conversations need indexing. The
Gateway verifies matching words against the current transcript before showing them.

The first enabled start creates two extension tables. Each foreign key has a
1.75-second PostgreSQL lock timeout. A 1.90-second deadline covers the whole
attempt, including consecutive locks, leaving room within its two-second startup
budget. If either key cannot be verified, the provider stays unavailable until
a later start. A trigger on the derived words table retires the conversation's
completion marker when a source deletion cascades through the words. This lets
the next search find an older visible revision of the same message. PostgreSQL
also runs this trigger for older clients. The trigger function can retire only
the marker for its original derived relation. Startup verifies the trigger and
clears old completion markers if it needs repair. Changed transcript views
are rebuilt under a new generation. Interrupted readers cannot mark a newer view
complete or restore words removed by a seat release.

Remove the plugin entry and restart the Gateway before discarding its derived
data. Use a quiet interval with conversation traffic paused. Dropping the two
foreign keys with their tables takes exclusive locks on the source message and
conversation tables until commit. Run these statements in order to preserve
the original conversations and remove the extension's trigger function.

```sql
DROP TABLE IF EXISTS hm_message_search_words;
DROP TABLE IF EXISTS hm_message_search_threads;
DROP FUNCTION IF EXISTS hm_message_search_forget_coverage();
```
