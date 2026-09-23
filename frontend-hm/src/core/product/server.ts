import { cache } from "react";

import { AUTH_REQUEST_TIMEOUT_MS } from "../auth/constants";
import { getGatewayConfig } from "../auth/gateway-config";
import { isStaticWebsiteOnly } from "../static-mode";

import { DEFAULT_PRODUCT_NAME, productNameFrom } from ".";

/**
 * The deployment's product name, read from the Gateway once per request.
 *
 * Public on the Gateway, so the sign-in page has it before anyone signs in.
 * Read on every request rather than cached, so an operator's edit to
 * `ui.product_name` reaches the next page load. A Gateway that does not
 * answer leaves the default: the name is never a reason for a page to fail.
 */
export const getServerSideProductName = cache(async (): Promise<string> => {
  if (isStaticWebsiteOnly()) {
    return DEFAULT_PRODUCT_NAME;
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), AUTH_REQUEST_TIMEOUT_MS);
  try {
    const { internalGatewayUrl } = getGatewayConfig();
    const response = await fetch(`${internalGatewayUrl}/api/product`, {
      cache: "no-store",
      signal: controller.signal,
    });
    return response.ok
      ? productNameFrom(await response.json())
      : DEFAULT_PRODUCT_NAME;
  } catch {
    return DEFAULT_PRODUCT_NAME;
  } finally {
    clearTimeout(timeout);
  }
});
