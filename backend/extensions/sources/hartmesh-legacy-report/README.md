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
