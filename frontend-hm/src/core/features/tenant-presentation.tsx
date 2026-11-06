"use client";

import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { z } from "zod";

import { fetch } from "@/core/api/fetcher";
import { useAuth } from "@/core/auth/AuthProvider";
import { getBackendBaseURL } from "@/core/config";
import { useProductIdentity } from "@/core/product/context";
import type { ProductIdentity } from "@/core/product/identity";
import { isStaticWebsiteOnly } from "@/core/static-mode";

const MAX_RESPONSE = 384 * 1024;
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const name = z
  .string()
  .min(1)
  .max(160)
  .refine(
    (v) =>
      Array.from(v).length <= 80 &&
      v === v.trim() &&
      !/[\p{Cc}\p{Cf}\p{Cs}\u2028\u2029]/u.test(v),
  );
const tenant = z
  .object({
    name,
    logo: z
      .string()
      .max(349550)
      .regex(/^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/),
  })
  .strict();
const strictSnapshot = z
  .object({
    schema_version: z.literal(1),
    mode: z.enum(["tenant", "product-only"]),
    product_sha256: digest,
    snapshot_id: digest,
    tenant: tenant.nullable(),
  })
  .strict();
export type TenantPresentation = z.infer<typeof strictSnapshot>;
type Selection = { mode: "legacy" } | TenantPresentation;
const TenantContext = createContext<Selection | null>(null);
export const useTenantPresentation = () => useContext(TenantContext);

/** Same ASCII canonical JSON as the mounted reader, including Unicode pairs. */
export function canonicalPresentation(value: unknown): string {
  const sort = (v: unknown): unknown => {
    if (Array.isArray(v)) return v.map(sort);
    if (v !== null && typeof v === "object") {
      return Object.fromEntries(
        Object.entries(v)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, item]) => [key, sort(item)]),
      );
    }
    return v;
  };
  return JSON.stringify(sort(value)).replace(
    /[\u007f-\uffff]/g,
    (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}

export async function presentationDigest(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalPresentation(value));
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(hash), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}

export async function parseTenantPresentation(
  body: unknown,
  product: ProductIdentity,
): Promise<Selection> {
  if (
    z
      .object({ schema_version: z.literal(1), mode: z.literal("legacy") })
      .strict()
      .safeParse(body).success
  )
    return { mode: "legacy" };
  const snapshot = strictSnapshot.parse(body);
  if ((snapshot.mode === "tenant") !== (snapshot.tenant !== null))
    throw new Error("Incomplete tenant presentation");
  const { snapshot_id, ...content } = snapshot;
  if (
    (await presentationDigest(product)) !== snapshot.product_sha256 ||
    (await presentationDigest(content)) !== snapshot_id
  )
    throw new Error("Tenant presentation binding mismatch");
  return Object.freeze({
    ...snapshot,
    tenant: snapshot.tenant ? Object.freeze(snapshot.tenant) : null,
  });
}

async function fetchSelection(
  product: ProductIdentity,
  signal: AbortSignal,
): Promise<Selection> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/branding/presentation`,
    { cache: "no-store", signal },
  );
  if (!response.ok) throw new Error("Tenant presentation unavailable");
  const reader = response.body?.getReader();
  if (!reader) throw new Error("Tenant presentation unavailable");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > MAX_RESPONSE)
        throw new Error("Tenant presentation exceeds limit");
      chunks.push(value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.length;
    }
    return await parseTenantPresentation(
      JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
      product,
    );
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

/** One complete selection for this authenticated account/document lifetime. */
export function TenantPresentationProvider({
  children,
}: {
  children: ReactNode;
}) {
  const product = useProductIdentity();
  const { user } = useAuth();
  const accountId = user?.id;
  const staticMode = isStaticWebsiteOnly();
  const [selection, setSelection] = useState<Selection | null>(
    staticMode ? { mode: "legacy" } : null,
  );
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    if (staticMode || !product || !accountId) return;
    let current = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      if (current) setFailed(true);
    }, 10_000);
    void fetchSelection(product, controller.signal)
      .then(
        (next) => {
          if (current && !controller.signal.aborted) setSelection(next);
        },
        () => {
          if (current) setFailed(true);
        },
      )
      .finally(() => clearTimeout(timeout));
    return () => {
      current = false;
      controller.abort();
      clearTimeout(timeout);
    };
    // AuthProvider remounts the entire protected subtree on account/role changes.
    // Client navigation, focus and server refreshes do not replace this selection.
  }, [product, accountId, staticMode]);
  if (!selection)
    return (
      <main className="flex min-h-screen flex-col items-center justify-center gap-4 bg-white p-4 text-black">
        <h1 role="status">
          {failed ? "Workspace identity unavailable" : "Loading workspace"}
        </h1>
        {failed && (
          <button
            className="rounded border border-black px-4 py-2"
            onClick={() => window.location.reload()}
          >
            Reload
          </button>
        )}
      </main>
    );
  return (
    <TenantContext.Provider value={selection}>
      {children}
    </TenantContext.Provider>
  );
}
