# Installed artifact presentations

Use the existing operator-managed `plugins:` installation and restart lifecycle.
Artifact files never select packages, module URLs or executable code. Ordinary
skills should emit passive `*.view.json` results using [the v1 contract](README.md).

The dependency-free `deerflow-extension-api` provides `ArtifactPresentation` for
`PluginContribution.artifacts`. Artifact-bearing contributions require
`api_version=2`; page-only contribution v1 remains supported. Older hosts reject
the unsupported contribution version explicitly. Each declaration has a unique
`id`, literal lowercase `suffixes`, `source_max_bytes` (at most 16 MiB) and
`preview_max_bytes` (at most 1 MiB). Optional `project(bytes) -> bytes` is a sync
module-level function importable by its module and name, with a literal
`projection_marker`. Optional compatibility query names
require a projector and must be globally unique. Core query names are reserved.

The authenticated artifact route accepts `preview=<namespace>/<id>` only for an
installed, enabled declaration. It authorizes the existing thread/read boundary
before reading an outputs file through a no-follow descriptor. A bounded hash
lookup may reuse a result. Each new projection uses the digest of the bytes
read under the worker slot. The Gateway rechecks access before delivery.

Projectors run in a short-lived isolated Python interpreter. A non-root
Gateway worker keeps the Gateway's OS identity. A root Gateway drops the
child to the unprivileged `nobody` account. Installed modules must be readable
by that identity. The worker searches the Gateway interpreter's configured
module paths. Projectors must be deterministic, single-threaded and read-only.
Treat installation as granting trusted code execution. The Linux host enforces
a 10-second deadline, CPU and address-space limits, and bounded pipe output.
At capacity the route returns HTTP 503 with detail code `preview-busy` and
`Retry-After`. A worker startup failure returns 501. Responses contain no parser
diagnostics. Successful previews carry `X-Artifact-Projection`,
`X-Artifact-Source-Bytes`, a full source SHA-256 ETag and `Cache-Control: no-store`.
`download=true` and ordinary reads always retain canonical source bytes.

The installed browser module retains `apiVersion: 1` and its old page-only
`surfaces` array. Add `artifactApiVersion: 1` and an independent `artifacts` array
whose IDs match backend declarations. An artifact has `id`, `title` and either:

- `kind: "passive"`, with `present(context)` returning a v1 view document,
  synchronously or as a Promise. The host applies the strict passive decoder.
- `kind: "native"`, with synchronous `mount(root, context)` returning
  `{ dispose() }`. It owns independent DOM, with no assumed shared React runtime.
  Optional `files: { exports: [{ path, label }], collection? }` reuses the host's
  explicit selected-file controls and recorded-presentation eligibility.

The context supplies installed namespace/settings, locale, theme, a cancellation
signal, namespace-bound backend actions, an immutable artifact snapshot
(`filepath`, `threadId`, `content`, full `revision`, `projected`, `presented`) and
bounded `loadRaster(relativePath)` for local PNG/JPEG resources. References remain
relative to the artifact directory. Source files and raster images are never
implicit exports. Native module privileges are those of trusted browser code;
Shadow DOM scopes styles and is not a security boundary.

The host retires supported work and disposes resources on account, thread, path,
module entry, revision, locale or theme change. Discovery and module reads have
byte bounds and finite deadlines. Page/action services use the same installed
snapshot; transcript reads use the Gateway's visible-only export. Invalid mounts,
late rejections and cleanup errors are contained per plugin. Duplicate suffix
matches refuse a hidden winner. Projection/render failures offer Download for binary sources and a bounded text
fallback for recognized text sources; authentication/path failures stay terminal. Code view, copy, editing
and download load canonical bytes and never save projection bytes.


Management backend actions/model tools declare `purpose="management"` and
negotiate `PluginContribution(api_version=3)`. Business contributions keep their
default purpose; existing v1/v2 declarations remain compatible. Management needs
active operator delegation plus attributed caller authority and optional namespace
write authorization at execution. Raw contributed management routes use the common
host guard. Discovery offers only admitted management actions. Enabling delegation
does not add an installer or permit arbitrary package/module sources.
