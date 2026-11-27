# Tenant bundle

A deployment can use one operator-owned directory for its customer's name,
picture, report colors, starter prompts and role briefs. The Gateway reads the
directory for authenticated workspace content. Sandboxes can mount the same
directory read-only at `/mnt/tenant` for skills to use in reports. Product
branding and browser titles come from the
[runtime product identity](RUNTIME_PRODUCT_IDENTITY.md).

```yaml
tenant_bundle:
  path: /srv/hartmesh/operator/tenant
```

| File | Fields and behavior |
| --- | --- |
| `brand.json` | `company_name` is a plain line of up to 80 characters. `logo` is the name of a PNG or JPEG file in the directory. `colors.primary` and `colors.secondary` are hex colors such as `#123456` and `#abcdef`. |
| `provider.json` | Optional legacy support metadata for installations without a configured product identity. `display_name` is an optional plain line of up to 80 characters. `support_url` is an absolute HTTPS URL without credentials, whitespace or backslashes, at most 2048 characters. The link appears in About. With a configured product identity, set support in its public provider payload. |
| `starters.json` | A list using the same rules as `ui.starters`. The Gateway still serves this field for client compatibility. |
| `roles.json` | A list using the same rules as `ui.roles`. |

The files are optional. The Gateway names the field and rule for malformed
inputs and degrades each field independently. For example, a missing logo still
allows a valid company name, and an unusable starter list falls back to the
configured prompts. The Gateway reads changes on subsequent requests. In Compose,
`gateway/render_config.py --check` reports which resources it can read and
lists any input problems. See the [Compose guide](../deploy/compose/README.md#tenant-bundle)
for file permissions and mounts.

Authenticated `GET /api/features` includes branding metadata with a private
no-store cache policy. The authenticated `GET /api/branding/logo` serves the
admitted image with a private five-minute cache, or returns 404 when there is
no usable logo. Reports capture their display inputs at the start of each
operation. See [offline report presentation](OFFLINE_REPORT_PRESENTATION.md).

## Browser verification

Use a disposable local Gateway and a production frontend build. Prepare a
synthetic bundle from the repository root with the backend's installed Python.

```bash
fixture=$(mktemp -d)
uv run --no-sync --project backend python backend/tests/support/tenant_bundle_live.py "$fixture"
export DEER_FLOW_CONFIG_PATH="$fixture/config.yaml"
export DEER_FLOW_EXTENSIONS_CONFIG_PATH="$fixture/extensions.json"
export DEER_FLOW_HOME="$fixture/home"
export DEER_FLOW_AUTH_DISABLED=0
export AUTH_JWT_SECRET="$(openssl rand -hex 32)"
uv run --no-sync --project backend python -m uvicorn --app-dir backend app.gateway.app:app --host 127.0.0.1 --port 18111
```

In another terminal, build and serve the frontend with
`DEER_FLOW_INTERNAL_GATEWAY_BASE_URL=http://127.0.0.1:18111`. Point Playwright
at that server.

```bash
HARTMESH_LIVE_TENANT_BUNDLE=1 PLAYWRIGHT_SKIP_WEB_SERVER=1 \
  PLAYWRIGHT_BASE_URL=http://127.0.0.1:18110 \
  python3 scripts/pnpm.py --project frontend-hm -- exec playwright test tests/e2e/tenant-bundle-live.spec.ts
```

To exercise a configured product, prepare a new fixture with
`--configured-product`, set `HARTMESH_PRODUCT_PAYLOAD="$fixture/product"` for
the Gateway, and set `HARTMESH_TEST_PRODUCT_NAME="Example Product"` and
`HARTMESH_TEST_PROVIDER_NAME="Example Provider"` when running Playwright.
