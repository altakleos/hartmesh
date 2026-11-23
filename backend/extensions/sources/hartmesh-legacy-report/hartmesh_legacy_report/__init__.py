"""Historical report compatibility, installed through the ordinary extension lifecycle."""

from pathlib import Path

from deerflow_extension_api import AgentScope, ArtifactPresentation, BrowserAssets, MiddlewarePlacement, Placement, PluginContribution, extension

from .projection import project_report


class _FreshnessContributor:
    def contribute_middlewares(self, app_store, context):
        from .freshness import ReportFreshnessMiddleware

        return (MiddlewarePlacement(ReportFreshnessMiddleware(), Placement.STANDARD, AgentScope.LEAD),)


@extension(api="0.2.5", name="legacy-report")
def install(registry, config):
    enabled = config.get("enabled", True)
    if type(enabled) is not bool or set(config) - {"enabled"}:
        raise ValueError("Configure only a boolean enabled value")
    accepted = registry.plugin(
        PluginContribution(
            namespace="hartmesh.legacy-report",
            title="Historical report presentation",
            description="Compatibility presentation and filing for existing report artifacts.",
            enabled=enabled,
            api_version=2,
            frontend=BrowserAssets("legacy-report.v1", Path(__file__).parent),
            artifacts=(
                ArtifactPresentation(
                    id="report",
                    suffixes=(".report.json",),
                    source_max_bytes=16 * 1024 * 1024,
                    preview_max_bytes=1024 * 1024,
                    project=project_report,
                    projection_marker="business-report-v1",
                    compat_queries=("report_preview",),
                ),
            ),
        )
    )
    if accepted is not True:
        raise RuntimeError("Historical report presentation requires the artifact plugin host")
    if enabled:
        registry.middlewares(_FreshnessContributor())
