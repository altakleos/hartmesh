import { z } from "zod";

import { fetch } from "@/core/api/fetcher";
import { readBoundedArtifactBytes } from "@/core/artifacts/response";
import { getBackendBaseURL } from "@/core/config";

import { awaitPluginOperation } from "./operations";

const contributions = z
  .array(
    z.object({
      namespace: z.string().regex(/^[a-z][a-z0-9_.-]{0,95}$/),
      viewer_id: z.string().nullable().optional(),
      storage_api_version: z.literal(1).nullable().optional(),
      human_input_api_version: z.literal(1).nullable().optional(),
      actor_kinds: z
        .array(z.enum(["human", "nonhuman"]))
        .max(2)
        .optional(),
      storage_capabilities: z
        .object({
          api_version: z.literal(1),
          available: z.boolean(),
          actor_kinds: z.array(z.enum(["human", "nonhuman"])).max(2),
          files: z.boolean(),
          provision: z.boolean(),
          grants: z.boolean(),
          mediated_mutations: z.boolean(),
          native_attachments: z.boolean(),
          quiesced_recovery: z.boolean(),
        })
        .nullable()
        .optional(),
      module: z.string().nullable(),
      backend_actions: z.array(z.string()).optional(),
      entry: z.string().nullable(),
      // Keep unknown transports so the loader can reject only that contribution.
      transport: z.string().nullable().optional(),
      title: z.string(),
      description: z.string(),
      settings: z.record(z.union([z.boolean(), z.number(), z.string()])),
      artifact_presentations: z
        .array(
          z
            .object({
              id: z.string().regex(/^[a-z][a-z0-9-]{0,63}$/),
              suffixes: z
                .array(z.string().regex(/^\.[a-z0-9][a-z0-9._-]{0,63}$/))
                .min(1)
                .max(16),
              source_max_bytes: z
                .number()
                .int()
                .min(1)
                .max(16 * 1024 * 1024),
              preview_max_bytes: z
                .number()
                .int()
                .min(1)
                .max(1024 * 1024),
              projection_marker: z
                .string()
                .regex(/^[a-z][a-z0-9.-]{0,95}$/)
                .nullable(),
            })
            .strict(),
        )
        .max(16)
        .optional(),
    }),
  )
  .max(64);

export const frontendExtensionsQueryKey = ["frontend-extensions"] as const;

export async function fetchFrontendExtensions(signal?: AbortSignal) {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (signal?.aborted) abort();
  signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(abort, 30_000);
  try {
    const response = await awaitPluginOperation(
      fetch(`${getBackendBaseURL()}/api/plugins`, {
        cache: "no-store",
        signal: controller.signal,
      }),
      controller.signal,
      (late) => {
        void late.body?.cancel().catch(() => undefined);
      },
    );
    if (!response.ok)
      throw new Error(`Frontend extensions unavailable (${response.status})`);
    const read = await readBoundedArtifactBytes(
      response,
      1024 * 1024,
      controller.signal,
    );
    if (read.truncated)
      throw new Error("Plugin discovery byte budget exceeded");
    controller.signal.throwIfAborted();
    return contributions.parse(
      JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(read.bytes)),
    );
  } finally {
    clearTimeout(timeout);
    signal?.removeEventListener("abort", abort);
  }
}
