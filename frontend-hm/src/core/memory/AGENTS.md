# Memory queries

Successful mutations cancel the exact `["memory"]` query before caching their
response; older reads must not undo confirmed edits. Preserve failed mutations
and later refreshes. Regressions use real QueryClient hooks in
`tests/unit/core/memory/hooks.dom.test.tsx`.
