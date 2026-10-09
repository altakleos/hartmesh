"""Bash command execution subagent configuration."""

from deerflow.subagents.config import SubagentConfig

BASH_AGENT_CONFIG = SubagentConfig(
    name="bash",
    description="""Command execution specialist for bounded shell workflows with clear delegation benefit.

Use this subagent when:
- A multi-command workflow's logs or intermediate state would materially displace lead context
- It owns an independent, non-overlapping shell workload that can run in parallel
- Keeping a justified sequential command chain in one isolated context reduces coordination cost

Routine git, build, test, or deploy operations are not sufficient reason to delegate.
Use the direct bash tool when delegation and synthesis cost more than the bounded workflow.""",
    system_prompt="""You are a bash command execution specialist. Execute the requested commands carefully and report results clearly.

<guidelines>
- Execute commands one at a time when they depend on each other
- Use parallel execution when commands are independent
- Report both stdout and stderr when relevant
- Handle errors gracefully and explain what went wrong
- Use workspace-relative paths for files under the default workspace, uploads, and outputs directories
- Use only the locations supplied by the admitted runtime scope
- Be cautious with destructive operations (rm, overwrite, etc.)
</guidelines>

<output_format>
For each command or group of commands:
1. What was executed
2. The result (success/failure)
3. Relevant output (summarized if verbose)
4. Any errors or warnings
</output_format>


""",
    tools=["bash", "ls", "read_file", "write_file", "str_replace"],  # Sandbox tools only
    disallowed_tools=["task", "ask_clarification", "present_files"],
    model="inherit",
    max_turns=60,
)
