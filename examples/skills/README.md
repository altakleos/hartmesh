# Standalone passive-result skills

`supplier-comparison/` and `procedure-summary/` are complete ordinary packages.
Each contains its own producer and bounded document writers; neither imports
HartMesh, the legacy report skill, or a plugin. Zip an individual package under
its declared directory name to produce a `.skill` archive. Install it through
the authenticated private-skill upload API after operator delegation. Merely
copying an archive into My Files or Shared never activates it.

The input examples in `inputs/` contain synthetic data. Producers run with:

```bash
python3 -B supplier-comparison/scripts/build.py --input inputs/quotes.json --output /tmp/comparison-example
python3 -B procedure-summary/scripts/build.py --input inputs/procedure.json --output /tmp/procedure-example
```

Use a sandbox with Python 3.12 or later. Comparison's workbook requires
`openpyxl`; procedure's DOCX uses only the standard library. Source JSON is
bounded and preserved byte-for-byte. Every result gets a new complete directory,
a passive view, successful explicit exports and the original source.

Portable PDFs use Courier/WinAnsi, eight complete pages and a 2 MiB limit.
Unsupported text or a writer failure omits only that format and produces an
authored notice. UTF-8 source and successful editable exports remain available;
no text is silently replaced and no page is clipped. Individual exports are
bounded at 16 MiB. A failed view leaves ordinary outputs accessible.

HartMesh renders and copies the declared files through its generic artifact
contract. These examples do not add frontend composition or domain permissions.
Quotation arithmetic and procedure wording remain producer-owned. They do not
verify the source facts or certify operational safety.

See the [qualification journey](../../docs/skill-result-qualification.md) for
real Docker/browser execution and the separate read-only AIO check.
