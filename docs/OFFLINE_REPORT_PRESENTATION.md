# Offline report presentation

A report build, prose update, render or explicit export captures the current
display inputs once. It uses the customer name, sanitized PNG or JPEG logo and
report palette from `--tenant` or the read-only `/mnt/tenant` bundle.
Missing or unusable fields degrade individually. The same operation can
read the [offline product envelope](OFFLINE_REPORT_PRODUCT_IDENTITY.md) for
separate product and provider attribution in HTML and PDF.

Logo capture accepts PNG or JPEG up to 2 MiB and 4096 pixels on each axis.
It decodes the image once and freezes a PNG of at most 256 KiB and 1024
pixels on each axis. The effective build preferences can override the report
colors. Prose and regeneration use the source report's verified effective
choices and preserve its report facts and authored content.

Use `--company` to name the report's subject. The tenant label and optional
product/provider attribution come from the captured display inputs. The
passive preview keeps a neutral appearance and links to the formats in the
bundle.

## Retained report record

Every new bundle keeps a `presentation.json` record of mode `legacy` together
with any referenced `presentation/tenant.png` and `product-identity.json`.
The bundle manifest verifies these members and binds the report's tenant label,
effective palette, image bytes and product digest. Record or asset tampering,
missing members and unsupported versions fail validation before reuse.

Historical retained reports with mode `tenant` or `product-only` remain
readable and valid. Opening or downloading any saved format returns its
existing bytes. Regeneration reads historical content and preferences, then
captures the current bundle separately for the new version. It keeps the old
directory and its presentation record intact.

HTML and PDF can be carried into a new bundle when the full presentation and
the report content match. DOCX requires compatible tenant branding and palette.
XLSX can be carried when the report facts are unchanged. New bundles advertise
the formats actually present. When chart inputs are retained, a changed palette
can redraw them without recalculating the report. An older report without the
values needed to redraw a chart can still be downloaded as saved. An affected
regeneration reports which inputs need rebuilding.

New renders stage privately and verify the current source under the publication
lock before publishing a complete bundle. The current pointer moves after the
bundle is published. Explicit single-file exports keep bundle members intact.
