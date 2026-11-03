# Sandbox audit command classification details

Detail for `SandboxAuditMiddleware`
(`packages/harness/deerflow/agents/middlewares/sandbox_audit_middleware.py`),
moved out of the middleware guide to keep that guidance chain within its size
budget. The middleware guide keeps the position model summary; regression
coverage lives in `tests/test_sandbox_audit_middleware.py`.

## Heredocs

Heredoc bodies are data: `_split_compound_command` records headers (`<<EOF`,
`<<-EOF`, `<<'EOF'`) and consumes their bodies verbatim, so a body line starting
`$(curl url)` isn't promoted to a command position. `<<<` (here-string, needs
look-ahead + look-behind) and a `<<` inside `$(( ))`/`(( ))` (bit shift,
arithmetic depth tracked with the quote flags) must not open one.

This is a heuristic, not shell parsing: an unterminated body consumes the rest
of the string, an unclosed `((` only disables heredoc detection, and the failure
direction is always toward *more* command positions, not fewer.

## Programs read from stdin or a process-substitution operand

An interpreter that reads its program from stdin (`bash < <(curl url)`) or is
handed a process substitution as its operand (`bash <(curl url)`) executes the
fetched bytes, so both are blocked. This is decided from the token list rather
than one pattern: a script operand (`bash script.sh < <(curl url)`,
`bash < <(curl url) consumer.sh`, `bash script.sh <(curl url)`) or a shell
noexec flag (`bash -n < <(curl url)`) keeps the fetched bytes as data, and
`.`/`source` never read a program from stdin because they require a filename.

## Warnings and the exit marker

Medium-risk warnings are inserted before a trailing `Exit Code: N` line, so
consumers that read the last line (the subagent executor) still see the exit
status, and the rebuilt message keeps `deerflow_tool_meta`, `artifact` and `id`.

## Known gaps

- A flag that takes its value as the next word (`bash -o pipefail < <(curl u)`,
  `bash --rcfile f <(curl u)`) can hide the program; its value is
  indistinguishable from a script argument in this heuristic.
- Two-step forms (`x=$(curl u); eval "$x"`) need dataflow analysis.
