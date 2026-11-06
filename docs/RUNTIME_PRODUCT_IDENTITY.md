# Runtime product and provider identity

A single Gateway/frontend build supports the default HartMesh identity and
operator-supplied product/provider identities. This shared, non-secret deployment
configuration is independent of the authenticated tenant bundle. It supplies no
accounts, authorization, credentials, tenant selection or assistant persona.

## Input and ownership

Set `HARTMESH_PRODUCT_PAYLOAD` in the **Gateway process** to an absolute directory
containing only `product.json` and `assets/`. Mount that directory read-only; do
not mount a tenant bundle there. POSIX filesystem semantics are required. The
frontend reads the Gateway; it needs no payload mount or brand-specific build.
For Compose, add a service override (use the Gateway service name in your profile):

```yaml
services:
  gateway:
    environment:
      HARTMESH_PRODUCT_PAYLOAD: /opt/product-identity
    volumes:
      - /absolute/operator/product-identity:/opt/product-identity:ro
```

`product.json` is strict UTF-8 JSON, at most 16 KiB:

```json
{
  "schema_version": 1,
  "public": true,
  "product": {
    "name": "Example Product One",
    "logo": "assets/product.png",
    "browser_icon": "assets/icon.png"
  },
  "provider": {
    "name": "Example Provider One",
    "logo": "assets/provider.png",
    "support_url": "https://support.example.test/one"
  }
}
```

Every field is required. There are no extension keys: unknown fields, duplicate
JSON members, nulls, unsupported versions and incomplete profiles reject. Names
are 1–40 Unicode code points, without outer whitespace, controls, surrogates,
formatting or line separators. The support URL is absolute HTTPS, at most 2,048
characters, without userinfo, whitespace, backslashes or encoded authority.

Assets must be complete static **PNG** files. Names match
`assets/[A-Za-z0-9_-]{1,80}.png`; paths, URLs and symlinks cannot select external
files. Each input and re-encoded asset is at most 256 KiB; combined unique input
and processed assets each have a 512 KiB ceiling. Logos are 1–1,024 pixels on each
side (at most 1,048,576 pixels); the browser icon is square, 32–256 pixels. Files
must be regular, singly linked, with no symlink ancestors. Animated PNG, SVG,
other image formats, trailing content, malformed files and unreferenced files
reject. The reader verifies/decompresses PNGs and re-encodes pixel data, stripping
metadata. A path may supply multiple roles if it satisfies all their bounds.

All declared names, the support destination and all three assets are explicitly
**public**, approved by the operator with `public: true`. They appear in the
anonymous product response, initial page and browser icon. Never put tenant or
credential material in them. Tenant branding, tenant assets and `/api/features`
retain their existing authentication boundary. No tenant directory is served by
the public reader. Public support opens in another tab without referrer or
implicit conversation/diagnostic sharing.

The identity fields (product name/logo/icon; provider name/logo/support;
schema/public declaration) are fixed by this payload. There are no mutable
identity defaults within a configured profile. `ui.product_name` and the Compose
`HARTMESH_PRODUCT_NAME` legacy input must be absent or agree with its product
name; conflicting values reject. A tenant `provider.json` is unauthorized in
configured mode, even if it repeats the public provider: migrate that information
to this payload and remove that legacy file. Tenant company data cannot override
product or provider slots. Credentials and operating settings are outside the
schema and cannot be overlaid through it.

Without `HARTMESH_PRODUCT_PAYLOAD`, existing `ui.product_name` (including Compose's
`HARTMESH_PRODUCT_NAME` rendering) still overrides the HartMesh name default.
Legacy names retain the existing `UiConfig` Unicode policy, including joiners;
the narrower mounted-payload name policy applies only in configured mode.
Existing tenant provider/support behavior remains unchanged. Existing
`ui.profile`, `ui.starters`, tenant company data and other operational defaults
remain independently configurable; they are not payload fields. Unknown payload
keys cannot override them. No custom colors are accepted; the established themes
and black-on-white provider/support controls preserve contrast.

## Activation, readers and failure

The Gateway freezes the first successfully validated complete payload and all
processed image bytes in memory. Restart **all Gateway workers** to activate a
replacement, including changing between default and configured mode. Use the
same directory/configuration for every worker. There is no watcher or lifecycle
controller. A failed initial load/configuration rejects startup; it never turns
into default identity. Once admitted, mount disappearance/changes do not alter
that process's snapshot. Ownership checks still reject conflicting legacy input.

`GET /api/v1/auth/product-identity` is public and returns `Cache-Control: no-store`:

- `schema_version: 1`, `mode: "configured" | "default"`, `name` (legacy alias);
- `product: {name, logo, browser_icon}`;
- `provider: {name, logo, support_url}` for configured mode, otherwise `null`.

Assets in this response are sanitized `data:image/png;base64,...` strings. Default
mode has null product assets and uses the shipped icon. Filesystem paths and raw
input documents are never returned. The legacy `/api/v1/auth/product` endpoint
continues returning `{name}` from the same effective identity. Unusable reads
return HTTP 503. The frontend validates one bounded response (2 MiB maximum,
auth-request deadline) per server request.

Sign-in/setup show the product and an explicitly public provider support control.
The authenticated sidebar shows the product mark/name plus provider support;
product information separately identifies the supplied product, provider and
truthful **HartMesh component version**. Page titles and icons use the product.
Configured product slots cannot be replaced by tenant fields. Tenant information
may still appear in tenant-owned content and authenticated APIs.

Each mounted frontend document retains one complete identity across navigation.
A reader outage preserves an established document's identity; a fresh page with
no validated identity displays **Service identity unavailable** and a reload
control, with no invented name, icon or support destination. Metadata uses the
same explicit unavailable state. Reload obtains the current Gateway snapshot.
Static-demo builds retain their built-in HartMesh identity and do not read the
Gateway. This requires matching Gateway/frontend versions; an older Gateway
without the full reader produces the explicit unavailable page.

## Verification

Backend: `uv run --no-sync --project backend pytest
backend/tests/test_product_identity.py backend/tests/test_product_router.py -q`.
Frontend: use `scripts/pnpm.py --project frontend-hm -- rstest run
 tests/unit/core/product tests/unit/app/product-name-layouts` with the worker and
disk-temporary limits in `frontend-hm/AGENTS.md`. The optional real-service browser
suite uses the documented environment in its own source and never bypasses auth.

Runtime product identity does not qualify generated reports, tenant image
preparation, tenant activation/restoration or browser tenant switching. Release
publication and artifact qualification are separate evidence.
