"""Identity helpers for GitHub webhook dispatch.

The routing helpers here provide both legacy development identity and the
owner-scoped verified identity used by durable webhook admission:

* :func:`resolve_thread_id` makes the langgraph thread id deterministic
  from ``(repo, number, agent_name)``. Same PR + same agent → same
  thread, even across gateway restarts. Different agents on the same PR
  (e.g. coder + reviewer) deliberately get different thread ids — see
  the function docstring for the rationale.

* :func:`resolve_conversation_identity` binds verified route evidence into a
  versioned conversation identity.
* :func:`extract_target` extracts the ``(repo, number)`` pair from a
  webhook payload, so the dispatcher can route deliveries to the right
  thread.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

from app.runtime.native_binding import (
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)

# UUID5 namespace dedicated to GitHub-driven threads. The bytes themselves
# are arbitrary; what matters is that every Gateway process uses the *same*
# namespace so a restart reproduces the same thread id. Don't change this
# without a migration plan.
GITHUB_THREAD_NAMESPACE = uuid.UUID("a3f4b2c1-7e8d-4f6a-b9c0-1234567890ab")


@dataclass(frozen=True, slots=True)
class GitHubConversationIdentity:
    """Versioned routing identity for one GitHub agent conversation."""

    version: int
    thread_id: str
    topic_id: str


def resolve_thread_id(repo: str, issue_or_pr_number: int, agent_name: str) -> str:
    """Build a deterministic langgraph thread id from a GitHub target + agent.

    The agent name is part of the seed so two agents bound to the same
    PR/issue (e.g. a coder + a reviewer on ``owner/repo#7``) land on
    distinct LangGraph threads. Sharing the thread would force
    ``multitask_strategy="reject"`` to silently drop one run on every
    dual-mention, and would couple the two agents' message histories
    and checkpoints. Each agent now owns its own thread; cross-agent
    coordination flows through GitHub (PR comments, review threads) —
    the source of truth humans see anyway.

    Args:
        repo: ``"owner/name"``.
        issue_or_pr_number: Issue or PR number (they share the namespace on
            the GitHub side, so we don't need to distinguish here).
        agent_name: The bound custom agent's canonical ASCII identity,
            validated upstream as ``[A-Za-z0-9][A-Za-z0-9-]{0,127}``.

    Returns:
        Stringified UUID5 under :data:`GITHUB_THREAD_NAMESPACE`.
    """
    if not isinstance(repo, str) or "/" not in repo:
        raise ValueError(f"Expected repo as 'owner/name', got {repo!r}")
    if not isinstance(issue_or_pr_number, int):
        raise ValueError(f"Expected issue_or_pr_number as int, got {type(issue_or_pr_number).__name__}")
    if not isinstance(agent_name, str) or not agent_name.strip():
        raise ValueError(f"Expected agent_name as non-empty str, got {agent_name!r}")
    return str(uuid.uuid5(GITHUB_THREAD_NAMESPACE, f"{repo}#{issue_or_pr_number}:{agent_name}"))


def resolve_conversation_identity(
    repo: str,
    issue_or_pr_number: int,
    agent_name: str,
    *,
    verified_binding: InternalVerifiedNativeBinding | None,
) -> GitHubConversationIdentity:
    """Resolve one stable, owner-scoped GitHub conversation identity.

    Verified webhook traffic uses the sealed route binding in its v2 identity,
    so equal repository/target/agent coordinates owned by different users cannot
    share a thread, channel mapping, FIFO, or checkpoint history. Explicitly
    unverified development traffic retains the legacy v1 identity.
    """
    legacy_thread_id = resolve_thread_id(repo, issue_or_pr_number, agent_name)
    if verified_binding is None:
        return GitHubConversationIdentity(
            version=1,
            thread_id=legacy_thread_id,
            topic_id=f"{issue_or_pr_number}:{agent_name}",
        )
    if verified_binding.kind is not InternalVerifiedNativeBindingKind.webhook_route:
        raise ValueError("verified GitHub conversation requires a webhook route binding")

    canonical = json.dumps(
        {
            "domain": "deerflow-github-conversation-v2",
            "binding_reference": verified_binding.reference,
            "repository": repo,
            "target_number": issue_or_pr_number,
            "agent_id": agent_name,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    return GitHubConversationIdentity(
        version=2,
        thread_id=str(
            uuid.uuid5(
                GITHUB_THREAD_NAMESPACE,
                f"github-conversation-v2:{digest}",
            )
        ),
        topic_id=f"github-conversation:v2:sha256:{digest}",
    )


def extract_target(event: str, payload: dict[str, Any]) -> tuple[str, int] | None:
    """Best-effort extraction of (repo, number) from a webhook payload.

    Returns ``None`` when the event has no associated issue/PR number
    (e.g. ``ping``, ``push``) or when the payload is malformed.
    """
    repo = (payload.get("repository") or {}).get("full_name")
    if not isinstance(repo, str):
        return None

    number: int | None = None
    if event == "pull_request":
        pr = payload.get("pull_request") or {}
        number = pr.get("number") or payload.get("number")
    elif event == "pull_request_review":
        pr = payload.get("pull_request") or {}
        number = pr.get("number")
    elif event == "pull_request_review_comment":
        pr = payload.get("pull_request") or {}
        number = pr.get("number")
    elif event == "issue_comment":
        number = (payload.get("issue") or {}).get("number")
    elif event == "issues":
        number = (payload.get("issue") or {}).get("number")
    else:
        return None

    if not isinstance(number, int):
        return None
    return repo, number
