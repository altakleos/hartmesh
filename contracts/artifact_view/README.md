# Passive artifact views

A skill can publish a `*.view.json` file beside its ordinary outputs. The
HartMesh artifact panel renders a valid `hartmesh.artifact-view`, version `1`,
using six fixed primitives: facts, text, lists, tables, raster images and
notices. Values, wording, attribution and formatting belong to the producer.
The view cannot declare code, actions, queries, HTML, stylesheets or a renderer.

See [the structural schema](view.schema.json). The frontend's strict decoder
also enforces literal reference paths, dimensions, duplicate exports and
aggregate budgets. Unsupported or invalid documents retain ordinary file
access. A presentation limit does not make an independently generated output
invalid.

## Document limits

| Resource                                                | Limit                  |
| ------------------------------------------------------- | ---------------------- |
| Serialized UTF-8                                        | 1 MiB                  |
| Blocks / exports / image blocks                         | 64 / 16 / 32           |
| Facts in one block                                      | 32                     |
| Table columns / body rows                               | 12 / 200               |
| All rendered table cells, including headers and footers | 5,000                  |
| Labels and headings / normal text and cells             | 256 / 4,096 characters |
| View body read deadline                                 | 20 seconds             |

Every table row and footer has exactly the declared column count. The host
never silently truncates a table. Use a concise view with an explicit omission
notice and complete exports when a result exceeds these bounds. The optional
six-digit hex accent affects decorative borders; readable text follows the
viewer theme. Notice tones are producer-authored presentation, without a
platform verification claim.

## Local references and file actions

Image and export paths are literal filenames relative to the view's directory,
including descendants. Absolute paths, schemes, hidden/empty/dot segments,
backslashes, controls, percent escapes, queries and fragments are refused.
Ill-formed Unicode references are also refused so URL encoding remains safe.
The optional `primary_source.path` names one distinct ordinary sibling file,
never another view. It does not grant file access or copy eligibility.

Each export supplies a path and a custom display label. Only server-recorded
presentations admit export controls; discovering an artifact or requesting a
failed presentation is insufficient. An explicitly presented directory covers
named descendants using the existing delivery rule. It does not enumerate or
copy the directory. Authenticated one-byte probes separately establish current
availability, with four concurrent probes and a ten-second deadline. Temporary
failures remain uncertain and offer Retry check.

Download operates on the named file. Save to My Files and Share use only the
selected available exports and the existing individual file operations,
partial-success messages and publication Undo. Sources and images are not
automatically copied. If an image or source is also an explicit presented
export, it can be selected normally. Saving outputs does not create a portable
rich bundle.

An optional `destination.collection` suggests one folder for these card
actions, displayed before the action. A schema-valid unsafe suggestion is
visibly ignored in favor of the ordinary destination. Private folder hierarchy
does not determine a company publication destination.

## Raster resources and lifecycle

Images must contain complete PNG or JPEG framing and pass the browser raster
decoder. Animated PNG is not supported. Filename extensions and response MIME
types do not establish content safety. Images are fetched through the existing
authenticated artifact service and displayed through host-owned raster Blob
URLs.

Each image is limited to 2 MiB, 4,000,000 decoded pixels and 8,192 pixels on
either axis. One displayed view retains at most 8 MiB and 16,000,000 decoded
pixels, with two concurrent loads, a finite queue and twenty-second deadlines.
The deadline includes queue time. A native decoder that outlives cancellation
keeps its concurrency slot and pixel reservation until its late bitmap is
closed; it cannot start additional work by timing out.
Failure stays local to the image; unrelated exports remain available.

Account, thread, path or observed content revision changes retire the displayed
session, cancel its queued work and revoke its URLs. Unused view and file-probe
queries are discarded. These controls establish current file availability and
display bounds, rather than file-copy transactions or correctness of authored
claims.
Rich views require an observed SHA-256 revision from the Gateway or secure
browser hashing. An older Gateway on an insecure origin without that revision
retains ordinary file access.
