> Upstream reference: examples of upstream UI and optional features may differ
> from HartMesh. See [HartMesh product scope](../FRONTEND_ISOLATION.md#product-scope)
> and [this adoption record](../plans/2026-11-16-upstream-adoption.md) for supported
> surfaces, adaptations and deferred behavior.

# File attachments and previews

## HartMesh Office file previews

When the Gateway configuration enables the installed Office presentation,
select an `.xlsx`, `.docx` or `.pptx` produced in an ordinary chat to read
it in the file panel. Download retrieves the complete original file.

- A workbook shows up to 200 rows and 12 columns per visible worksheet, within
  a shared 5,000-cell budget. Formula cells show their saved value when present
  and their formula text when a saved result is missing.
- A Word document shows its body text, lists and tables in reading order.
- A slide deck shows the text and speaker notes of up to 60 visible slides.

The preview reports omissions. Pictures, charts and embedded objects remain in
the original download. AI employee and assistant conversations, attachments,
My Files and Shared retain their file download behavior.

## Attachments and downloads

Attachments show individual ready, uploading, uploaded, failed and cancelled states.
Failed uploads retain per-file error details; session-state messages remain localized.
Retry a failed file or remove it before sending again; successful uploads and the
message draft are retained. File cards share format icons and preserve extensions
in long filenames. Sent attachments open in the right-hand artifact panel.
Single-file downloads show preparation and retry feedback while preventing duplicate
requests. Unsupported formats offer the original file for download.
Public Showcase files keep their standalone viewer and download controls available
without requiring sign-in.

Deployments can add binary readers through the optional `artifactPreviews` browser
contribution; see the [plugin contract](../full-stack-plugins.md#artifact-previews).

## Upstream Office browser extension

In upstream DeerFlow, the optional [Office preview package](../../examples/deerflow-extension-office-preview/README.md)
adds read-only XLSX, DOCX and PPTX readers with search, worksheet/page navigation,
Word outlines, PowerPoint thumbnails and per-file reading preferences. It requires
the artifact-preview host contribution and operator installation; it is not enabled
by a catalog entry alone. Legacy binary Office files retain the download fallback.
Previews also work over LAN HTTP. Reading preferences use the file's ETag when
available, with an optional content hash on secure origins; without either, files
still open but reading preferences are not restored.
Office preview validates ZIP metadata before loading, prepares files in a cancellable
Worker with a deadline, and limits XML/layout and image sizes before DOM rendering.
Word XML/model preparation and PPT conversion run in that Worker; the UI receives
bounded models with images and document relationships preserved.
Files exceeding preview budgets retain the original-file download fallback.

See the [Office package guide](../../examples/deerflow-extension-office-preview/README.md)
for installation, supported formats, resource limits and validation commands.
