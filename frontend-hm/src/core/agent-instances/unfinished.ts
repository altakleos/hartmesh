import { z } from "zod";

import { safeLocalStorage } from "@/core/settings/local";

import type { CompanyCopyRequest, CreateInstance, Custody } from "./api";

export type RememberedCreation =
  | { body: CreateInstance; definition_name: string | null; copy_of: null }
  | { body: CompanyCopyRequest; definition_name: null; copy_of: string };

const identity = z.string().regex(/^[0-9a-f]{32}$/);
const common = {
  creation_id: identity,
  name: z.string().min(1).max(128),
  supervisor_id: z.string().min(1).optional(),
};
const creation = z.union([
  z
    .object({
      body: z
        .object({
          ...common,
          definition_name: z.string().min(1),
          custody: z.enum(["personal", "company"]),
        })
        .strict(),
      definition_name: z.string().min(1).nullable(),
      copy_of: z.null(),
    })
    .strict(),
  z
    .object({
      body: z
        .object({ ...common, generation: z.number().int().nonnegative() })
        .strict(),
      definition_name: z.null(),
      copy_of: identity,
    })
    .strict(),
]);

function key(userId: string) {
  return `deerflow.agent-instances.unfinished.${userId}`;
}

export function rememberCreation(
  userId: string,
  request: RememberedCreation,
): boolean {
  return safeLocalStorage.setItem(key(userId), JSON.stringify(request));
}

export function readCreation(userId: string): RememberedCreation | null {
  const raw = safeLocalStorage.getItem(key(userId));
  if (!raw) return null;
  try {
    const parsed = creation.safeParse(JSON.parse(raw));
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}

export function matchesCreation(
  request: RememberedCreation,
  entry: { name: string; custody: Custody },
): boolean {
  const custody = request.copy_of === null ? request.body.custody : "company";
  return request.body.name === entry.name && custody === entry.custody;
}

export function clearCreation(userId: string): boolean {
  return safeLocalStorage.removeItem(key(userId));
}
