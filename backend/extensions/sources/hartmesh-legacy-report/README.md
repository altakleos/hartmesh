# Historical report compatibility

This provider-installed package owns historical `.report.json` presentation:
its parser, decimal-spelling formatter, native DOM and styles, translations,
companion filenames and legacy filing policy. It mounts independently of React
and imports no host frontend source. The generic host supplies authenticated
file controls and bounded PNG/JPEG loading.

The package's pure bytes projector keeps display fields, leaving source rows and
build provenance in the canonical file. The host authorizes and captures that
source, limits reads to 16 MiB and projections to 1 MiB, hashes the captured bytes
and drains the worker before retirement. `report_preview=true` is an installed
compatibility alias; the generic host contains no report-specific dispatch.

The product's locked extensions group includes this package. Provider configuration
must explicitly activate it through the existing lifecycle:

```yaml
plugins:
  - name: legacy-report
    package: hartmesh-legacy-report
    use: hartmesh_legacy_report:install
    enabled: true
    required: false
    config:
      enabled: true
```

Existing configurations need this entry and a Gateway restart. Missing or disabled
adapters retain ordinary source/download access. Installation, upgrade, enable,
disable and removal use the normal `deerflow extensions` / `make extension-*`
commands and restart semantics; no separate browser installer is introduced.

Only explicitly presented PDF/Word/Excel companions can be offered or copied, and
host controls verify live availability. The collection hint is `Reports`. Ordinary
file actions consult the installed filing callback, which recognizes presented
report companions and flat `/mnt/user-data/files/Reports/<leaf>` row sharing.
Other personal folders and nested legacy folders are not copied into Shared.

The native package keeps the container-based 10rem KPI floor and numeric formatting
parity with the skill. Keep the browser's one-line monetary KPI, own-tile and overflow
assertions, plus mobile, theme, missing-chart, current-render and canonical-edit tests.
Changing report bytes changes their revision and retires the old renderer; unchanged
source/revision rerenders retain the mounted chart state.

Rebuild the portable parser, formatter and path modules with the repository's existing compiler:

```bash
node backend/extensions/sources/hartmesh-legacy-report/build-browser.mjs
```

The committed `.mjs` assets are delivered from the installed package's revisioned
manifest. Rebuilding them does not rebuild the HartMesh frontend.

### Live inputs on the next turn

The installed lead-agent middleware checks the latest delivered business report
in the last 128 messages of an ordinary web chat. The Gateway selects the hook
for the lead graph before assembly. Telegram and output-captured graphs keep
their original presentation inventory. A handoff under
`outputs/reports/…/drafts/<id>/` must name the passive view or canonical report.
The capture verifies the selected member hashes and reads the compact hashed
`input-sources.json` bound to the canonical digest. It retains
the source paths, SHA-256s, scoped mount identities and file revisions on the
host-tagged handoff. The settled reply identifies a changed live input and offers
an explicit rebuild. No step changes the report or loads new figures.

The check follows only exact My files and Shared paths. Local sandbox host
spellings must belong to its user's exact mount. Native reads qualify the
current User and Company Spaces on the owning loop, open through confined file
descriptors and recheck authority before reporting a change. A re-presented
or retained-facts native draft requires a matching prior handoff in the original
Space. Only a fresh build's producing tool can establish a new native baseline. An unavailable,
revoked or replaced mount, employee Home, remote thread-only mount, import or
upload supplies no live evidence. Ordinary reads also refuse symlink components.

Budgets per observation are 64 KiB for the manifest, 1 MiB for a passive view,
128 KiB for the compact provenance, 16 MiB for a canonical-only or older bundle
read and 100 MiB across up to ten inputs. Large canonical row payloads incur no
read for new view handoffs. An oversized draft or an admitted input that exceeds
the comparison budget offers an explicit check.
A matching file revision needs only a fresh descriptor stat. Missing evidence,
compacted handoffs, older unmarked presentations and inputs outside these bounds
keep normal chat available. Use the skill's explicit checks for those drafts.
