"""The employee proposal form for tracked assignments."""

from deerflow_extension_api import extension
from deerflow_extension_api.plugins import BackendAction, ModelTool, PluginContribution, PluginToolResult


async def submit(payload, context):
    if context.submit_proposal is None or set(payload) != {"enabled"} or type(payload["enabled"]) is not bool:
        raise ValueError("The tracked assignment form is unavailable")
    receipt = await context.submit_proposal("work_policy.enabled", payload["enabled"])
    action = "enable" if payload["enabled"] else "disable"
    return PluginToolResult(f"The proposal to {action} tracked assignments was submitted for review in the employee’s Settings. The setting changes only after a manager adopts it.", {"configuration_proposal": {"version": 1, **receipt}})


async def review(payload, context):
    if context.review_proposal is None or set(payload) != {"instance_id", "proposal_id"}:
        raise ValueError("Use a manager session to review this proposal")
    return await context.review_proposal(payload["instance_id"], payload["proposal_id"])


@extension(api="0.2.15", name="work-proposals")
def install(registry, config):
    registry.plugin(
        PluginContribution(
            namespace="hm.work-proposals",
            title="Tracked assignment proposals",
            enabled=config.get("enabled", True),
            api_version=5,
            proposal_api_version=1,
            proposal_tool=ModelTool(
                "propose_tracked_work",
                "Submit a proposal for a manager to turn this employee's tracked assignments on or off. Use this when the person wants to change whether this employee can receive assignments. The manager decides in employee Settings.",
                {"type": "object", "properties": {"enabled": {"type": "boolean"}}, "required": ["enabled"], "additionalProperties": False},
                submit,
                response_format="content_and_artifact",
            ),
            backend=(BackendAction("review", review),),
        )
    )
