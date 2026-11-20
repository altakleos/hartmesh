"""Read-only workbook, document and slide presentations."""

from pathlib import Path

from deerflow_extension_api import ArtifactPresentation, BrowserAssets, PluginContribution, extension

from .documents import project_document
from .slides import project_deck
from .workbooks import project_workbook


@extension(api="0.2.5", name="office-preview")
def install(registry, config):
    enabled = config.get("enabled", True)
    if set(config) - {"enabled"} or type(enabled) is not bool:
        raise ValueError("Configure only a boolean enabled value")
    artifacts = tuple(
        ArtifactPresentation(
            id=kind,
            suffixes=(suffix,),
            project=project,
            source_max_bytes=16 * 1024 * 1024,
            preview_max_bytes=1024 * 1024,
            projection_marker=f"office-{kind}-v1",
        )
        for kind, suffix, project in (
            ("workbook", ".xlsx", project_workbook),
            ("document", ".docx", project_document),
            ("deck", ".pptx", project_deck),
        )
    )
    accepted = registry.plugin(
        PluginContribution(
            namespace="hartmesh.office-preview",
            title="Office file previews",
            description="Read the text and tables in Office files.",
            enabled=enabled,
            api_version=2,
            frontend=BrowserAssets("office-preview.v1", Path(__file__).parent),
            artifacts=artifacts,
        )
    )
    if accepted is not True:
        raise RuntimeError("Office previews require the artifact presentation host")
