"""Small, run-local descriptions of admitted capabilities; never authority.

No provider construction, filesystem discovery or client context decoding belongs
here. The worker owns environment admission; every actual operation rechecks it.
"""

from dataclasses import dataclass
from typing import Literal

ASSIGNMENT_QUESTION_RULE = (
    "- This run works on an assignment, and no person is waiting in this conversation. "
    "When you need information or a decision from a person, call `report_work` with the action `request_input` before you stop: "
    "that puts your question in front of the supervisor. "
    "Never end your turn with a question in prose, and a `BLOCKED` result is complete only after that report."
)


@dataclass(frozen=True)
class RuntimeScope:
    mode: Literal["conversation", "instance", "unavailable"]
    tools: frozenset[str]
    memory: Literal["ordinary", "instance", "disabled"]
    agent_role: Literal["AI", "AI assistant", "AI employee"] = "AI"

    def working_directory(self, *, delegated=False, ordinary_extras="", libraries="") -> str:
        lines = ['<working_directory existed="true">']
        if self.mode == "unavailable":
            return "<working_directory>Instance storage is not qualified for this assembly. No requester filesystem fallback is available.</working_directory>"
        if self.mode == "instance":
            lines += [
                "- Your own files are at `/mnt/spaces/home`; files here persist across conversations.",
                "- Default working directory: `/mnt/spaces/home`. Its children `uploads/` and `outputs/` hold inputs and output files.",
                "- Compatibility aliases: `/mnt/user-data/workspace` names your own files; `/mnt/user-data/uploads` and `/mnt/user-data/outputs` name those children.",
                "- Shared: `/mnt/user-data/shared`, files anyone at the company published for everyone, read-only. You cannot put anything here; a person uses Share with everyone in the app.",
                "- File audience follows the current permissions on your files; it is neither requester-private nor automatically company-wide. Your file access does not grant a human access.",
                "- Only your own files and Shared are mounted. The requester's My Files and Project directories are not inherited. A referenced external file is not automatically an input here.",
                "- Your files are ordinary mutable data, separate from adopted instructions and recalled memory. Clearing memory does not delete files.",
                "- If a write fails because there is no space left, say that your files are full and that a person who can manage you can delete files on your Files tab.",
            ]
            if "report_work" in self.tools and not delegated:
                lines.append(ASSIGNMENT_QUESTION_RULE)
        else:
            lines += [
                "- Files are conversation-scoped; this does not promise deletion at run completion or automatic reuse in another conversation.",
                "- Treat `/mnt/user-data/workspace` as your default current working directory for coding and file-editing tasks.",
                "- Input files: `/mnt/user-data/uploads`; output files: `/mnt/user-data/outputs`.",
            ]
            if "list_uploaded_files" in self.tools:
                lines.append("- Current uploads appear in `<current_uploads>`. Use `list_uploaded_files` for historical uploads; converted Markdown may accompany office documents.")
            if ordinary_extras:
                lines.append(ordinary_extras)
        file_tools = sorted(self.tools & {"ls", "glob", "grep", "read_file", "write_file", "str_replace"})
        if file_tools:
            lines.append("- Available file tools: " + ", ".join(f"`{name}`" for name in file_tools) + ". Use only these admitted actions.")
        if "bash" in self.tools:
            prefix = "" if self.mode == "instance" else "../"
            lines.append(
                f"- When writing scripts or commands that create/read files from the workspace, use `hello.txt`, `{prefix}uploads/data.csv`, and `{prefix}outputs/result.md`. These paths are relative to the default working directory."
            )
            if libraries:
                lines.append(libraries)
        elif "write_file" in self.tools:
            lines.append("- No `bash` tool is bound: work out results directly and write them with `write_file` instead of saving helper scripts")
        if delegated:
            if self.mode == "instance":
                lines.append("- This delegated task inherits the parent's mounted files and filesystem authority. A narrower tool list does not narrow that native filesystem scope.")
            lines.append("- Report useful file paths to the parent; a delegated task cannot present files itself.")
        elif "present_files" in self.tools or "bash" in self.tools:
            lines.append("- Final deliverables belong in `/mnt/user-data/outputs`. Skill drafts remain ordinary artifacts until explicitly installed through authorized skill management.")
            if "bash" in self.tools:
                lines.append("- When a `bash` command writes a deliverable, name it under `present` in that same call for output validation and reference registration.")
            if "present_files" in self.tools:
                lines.append("- Use `present_files` to register an existing output reference. Registration alone does not verify existence or file contents.")
            lines.append('- "Presented to the user" means output references were registered. Do not register them again. Validation reports observed metadata, not immutable bytes or a completed human retrieval. ')
            lines.append(
                "- Current READ and explicit download EXPORT permissions on your files still apply; retrieval may fail or be denied."
                if self.mode == "instance"
                else "- Human retrieval still requires current conversation access and may fail or be denied."
            )
        lines.append(f"- Recalled memory scope: {self.memory}. Memory does not replace exact saved documents.")
        lines.append("</working_directory>")
        return "\n".join(lines)


def build_runtime_scope(*, execution=None, tools=(), memory_enabled=False) -> RuntimeScope:
    """Project only the exact host binding and its prepared environment."""
    from deerflow.agent_instances.runtime import InstanceSandboxProvider, current_environment

    names = frozenset(tool.name for tool in tools if isinstance(getattr(tool, "name", None), str))
    if execution is None:
        return RuntimeScope("conversation", names, "ordinary" if memory_enabled else "disabled")
    custody = getattr(getattr(execution, "instance", None), "custody", None)
    role = "AI assistant" if custody == "personal" else "AI employee" if custody == "company" else "AI"
    environment = current_environment()
    qualified = environment is not None and environment.execution is execution and isinstance(environment.provider, InstanceSandboxProvider) and environment.provider.execution is execution
    if not qualified:
        return RuntimeScope("unavailable", frozenset(), "disabled", role)
    return RuntimeScope("instance", names, "instance" if memory_enabled else "disabled", role)
