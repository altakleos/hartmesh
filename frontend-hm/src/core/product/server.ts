import { cache } from "react";

import { AUTH_REQUEST_TIMEOUT_MS } from "../auth/constants";
import { getGatewayConfig } from "../auth/gateway-config";
import { isStaticWebsiteOnly } from "../static-mode";

import {
  DEFAULT_IDENTITY,
  IDENTITY_UNAVAILABLE,
  parseProductIdentity,
  type ProductIdentity,
} from "./identity";

/** Read one bounded public snapshot per server request; failure never invents identity. */
export const getServerSideProductIdentity = cache(
  async (): Promise<ProductIdentity | null> => {
    if (isStaticWebsiteOnly()) return DEFAULT_IDENTITY;
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      AUTH_REQUEST_TIMEOUT_MS,
    );
    try {
      const { internalGatewayUrl } = getGatewayConfig();
      const response = await fetch(
        `${internalGatewayUrl}/api/v1/auth/product-identity`,
        {
          cache: "no-store",
          signal: controller.signal,
        },
      );
      if (!response.ok || !response.body) {
        await response.body?.cancel();
        return null;
      }
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          size += value.length;
          if (size > 2 * 1024 * 1024) {
            await reader.cancel();
            return null;
          }
          chunks.push(value);
        }
      } finally {
        reader.releaseLock();
      }
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }
      return parseProductIdentity(
        JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
      );
    } catch {
      return null;
    } finally {
      clearTimeout(timeout);
    }
  },
);
export const getServerSideProductName = cache(
  async () =>
    (await getServerSideProductIdentity())?.name ?? IDENTITY_UNAVAILABLE,
);
export async function productMetadata() {
  const snapshot = await getServerSideProductIdentity();
  return {
    title: snapshot?.name ?? IDENTITY_UNAVAILABLE,
    icons: {
      icon:
        snapshot?.product.browser_icon ??
        (snapshot ? "/favicon.ico" : "data:,"),
    },
  };
}
