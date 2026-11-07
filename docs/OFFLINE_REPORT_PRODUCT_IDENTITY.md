# Shared product attribution in offline reports

The business-report skill adds a separate product/provider block to configured
**HTML and the PDF printed from that HTML**. It contains the product name/logo,
provider name/logo and a named support link with its readable destination. The
report company/issuer, facts, charts, preferences and input provenance keep their
existing meaning. The browser icon is validated but is not printed.

DOCX, XLSX and the passive view do not add shared attribution. Complete tenant
selection, chart compatibility and regeneration are now governed by the
[offline presentation contract](OFFLINE_REPORT_PRESENTATION.md); shared identity
remains independently owned.

## Export and read-only exposure

The operator exports the complete identity using the **same payload, environment
and effective configuration as the Gateway**, with its installed backend:

```bash
HARTMESH_PRODUCT_PAYLOAD=/absolute/operator/product-payload \
DEER_FLOW_CONFIG_PATH=/absolute/operator/config.yaml \
uv run --no-sync --project backend python -m deerflow.config.product_identity_export \
  > /absolute/new-staging/product-identity.json
```

Use a new staging directory, outside active mounts. The exporter applies the
[runtime product contract](RUNTIME_PRODUCT_IDENTITY.md), including fixed-field
conflicts. Ordinary export refuses unconfigured mode. Explicit `--include-default` may
export the exact admitted default identity with null assets/provider for strict
tenant binding; it does not invent provider attribution. The output is public data only: strict UTF-8 JSON with exactly `schema_version: 1`,
`identity` (the entire configured public API response, including all three inline
sanitized PNGs when configured), and `sha256`. The digest is lowercase SHA-256 over the identity
encoded as JSON with sorted keys, comma/colon separators, ASCII escapes and no
non-finite numbers. It binds names, assets and support together. It is a content
checksum, not a signature, authorization token or deployment-generation service.

Read the record's `sha256` and compare the complete projected `identity` with the
intended Gateway identity before admitting readers. Mount the directory read-only
and set **both** selectors through existing operator sandbox configuration:

```yaml
sandbox:
  mounts:
    - host_path: /absolute/operator/admitted-product
      container_path: /mnt/product
      read_only: true
  environment:
    HARTMESH_REPORT_PRODUCT_IDENTITY: /mnt/product/product-identity.json
    HARTMESH_REPORT_PRODUCT_SHA256: <sha256 from the validated export>
```

Keep the existing sandbox provider/image and its other settings. In local Docker
AIO mode the mount source must be visible to the Docker daemon. When Gateway is
itself containerized, configure the source path as required by the existing
sandbox mount interface; a Gateway-only payload mount does not automatically
expose it to report containers. Remote/provisioner providers need their existing
operator-controlled read-only volume mechanism to expose these same bytes; they
are not qualified by the Docker proof below. A LocalSandbox uses a real absolute
host path as its environment selector and operator-enforced filesystem access;
its virtual file mappings alone are not a shell isolation/read-only boundary.

The reader requires no Gateway package, HTTP endpoint or live service. It uses
Python 3.10's standard library and the sandbox's shipped Pillow. It installs
nothing, launches no shell and loads no remote resources. The PDF printer still
refuses non-inline resources. The support URL is a reader destination only.
Neither report JSON, `--company`, report preferences nor a tenant file selects or
overrides shared attribution. Protect the mounted projection and environment
from writers, just as other operator deployment configuration.

## Validation and operation boundary

The projection is at most 2 MiB and must be a regular singly linked file selected
by a normalized absolute POSIX path, without symlink ancestors. Unknown/duplicate
fields, unsupported versions, incomplete projections and digest mismatch
refuse. Both embedded and expected digests must equal the recomputed identity.
A selected but missing/unreadable file is an error, never a default-mode signal.

The reader enforces the configured runtime name and HTTPS support rules. Each
inline PNG is at most 256 KiB; unique embedded PNGs total at most 512 KiB. Logos
are at most 1,024 by 1,024 pixels; the browser icon is square, 32–256 pixels. PNGs
must be complete static RGBA pixel projections with only IHDR/IDAT/IEND chunks.
The reader checks framing/CRC, decodes and re-encodes pixels with bounded output,
and rejects ancillary metadata, animation, malformed data and trailing content.
It retains the validated already-sanitized source bytes, preserving their digest
across different supported Pillow encoder versions. Display text is escaped.
No path or remote URL can supply an image. An explicit exact default-mode
projection instead requires null assets/provider and the actual validated legacy
product name; it is retained for binding without printing a provider block.

`build`, `prose`, `render` and explicit single-file export capture the complete
identity once before preparation or rendering. All selected formats use that
immutable in-memory result even if the source subsequently disappears or changes.
The next command validates its selected input again. Invalid configured input
fails before publishing a successful new bundle/export and preserves retained
outputs and current state. Existing publication locking, source comparison,
durable staging and uncertain-pointer outcomes remain unchanged.

Configured bundles retain the bounded canonical projection as the supporting
`product-identity.json` member under existing `renders.json` SHA-256 integrity.
Report and passive-view schemas remain unchanged. A later render uses its current
operator-selected shared identity; this member is historical evidence, not an
implicit historical-selection authority. If shared identity changes (including
configured/default transitions), previous HTML/PDF cannot be carried into a new
bundle as current formats. They must be selected for rendering or are omitted;
unchanged DOCX/XLSX remain eligible for existing verified carry. Old bundles and
downloads keep their bytes. This comparison also participates in the complete presentation contract;
tenant/palette changes separately fence DOCX and presentation-dependent charts.

Without either report selector and without a locally inherited
`HARTMESH_PRODUCT_PAYLOAD`, legacy shared-attribution behavior has no invented provider. Complete tenant
regeneration follows the offline presentation contract. A partial selector pair, or an inherited configured Gateway payload
without a projection pair, refuses. For container deployments the operator must
explicitly provide the pair whenever shared attribution is configured; a sandbox
cannot discover its Gateway's environment or prove that an omitted configuration
was intended. Removing both selectors is an operator choice of legacy reports.
Older skills ignore these selectors and are incompatible with configured report
attribution: admit matching reader/skill/runtime versions together.

## Activation

Stage and validate the payload and exported projection while readers are stopped.
Serialize activation outside report commands. Stop/drain affected report commands,
recreate all affected sandbox containers with the selected immutable mount and
expected digest, and restart every Gateway worker when its product payload
changes. A new host directory or file replacement does not update an existing
container's bind or environment. Verify each reader's selected bytes/digest and
the Gateway's complete effective identity before declaring the pair admitted.
Do not declare a mixed or uncertain state ready. Recovery and authorization remain
operator responsibilities; the exporter and report command do not restore or
activate deployment state. Do not edit active inputs in place.

## Verification

Focused host tests run from the repository root:

```bash
uv run --no-sync --project backend pytest \
  backend/tests/test_product_identity.py \
  backend/tests/skills/business_report/test_product_attribution.py -q
```

The reusable real-image qualification uses the same skill and image for default
and two synthetic profiles, ordinary report commands, independently summed CSV
facts, actual PDF text/link/embedded logo pixels and page bounds, explicit
exports, retention and failure cases. Fixtures are cross-checked against the
Gateway exporter in unit tests. The image has no installed Gateway package:

```bash
docker run --rm --network none --cap-drop ALL \
  --security-opt no-new-privileges --user 1000:1000 --entrypoint python3 \
  --volume "$PWD/skills/public/business-report:/mnt/skill:ro" \
  --volume "$PWD/backend/tests/skills/business_report:/mnt/qualification:ro" \
  <qualified-sandbox-image@sha256:digest> \
  /mnt/qualification/check_product_reports_on_image.py \
  --skill /mnt/skill --fixtures /mnt/qualification/fixtures \
  --output /tmp/report-product-qualification
```

Sandbox image smoke CI runs this against its actual built image with networking
disabled. Its proof is local offline document rendering and read-only exposure,
not tenant lifecycle, native storage containment or release artifact admission.
Release evidence must separately identify the exporter, skill and delivering
sandbox/runtime dependencies and mounts.
