import { z } from "zod";

export const ARTIFACT_VIEW_MAX_BYTES = 1024 * 1024;
export const ARTIFACT_VIEW_MAX_CELLS = 5000;
export const ARTIFACT_VIEW_MAX_IMAGES = 32;

export function isArtifactViewPath(path: string): boolean {
  return path.toLowerCase().endsWith(".view.json");
}

export function isArtifactViewRevision(revision: string | undefined): boolean {
  return revision !== undefined && /^[0-9a-f]{64}$/i.test(revision);
}

function isWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (code >= 0xdc00 && code <= 0xdfff) return false;
  }
  return true;
}

export function isSafeViewReference(path: string): boolean {
  return (
    path.length > 0 &&
    isWellFormedUnicode(path) &&
    !/[\p{Cc}\\:%?#]/u.test(path) &&
    path
      .split("/")
      .every((segment) => segment !== "" && !segment.startsWith("."))
  );
}

function text(max: number, required = false) {
  return z.string().refine((value) => {
    const length = Array.from(value).length;
    return length <= max && (!required || length > 0);
  });
}

const heading = text(256).optional();
const path = text(1024, true).refine(isSafeViewReference);
const facts = z
  .object({
    type: z.literal("facts"),
    heading,
    items: z
      .array(
        z
          .object({
            label: text(256, true),
            value: text(4096),
            detail: text(4096).optional(),
          })
          .strict(),
      )
      .min(1)
      .max(32),
  })
  .strict();
const paragraphs = z
  .object({
    type: z.literal("text"),
    heading,
    paragraphs: z.array(text(4096)).min(1).max(16),
  })
  .strict();
const list = z
  .object({
    type: z.literal("list"),
    heading,
    ordered: z.boolean().optional(),
    items: z.array(text(4096)).min(1).max(32),
  })
  .strict();
const table = z
  .object({
    type: z.literal("table"),
    heading,
    columns: z
      .array(
        z
          .object({
            label: text(256, true),
            align: z.enum(["start", "end"]).optional(),
          })
          .strict(),
      )
      .min(1)
      .max(12),
    rows: z.array(z.array(text(4096)).min(1).max(12)).max(200),
    footer: z.array(text(4096)).min(1).max(12).optional(),
  })
  .strict();
const image = z
  .object({
    type: z.literal("image"),
    heading,
    path,
    alt: text(256, true),
    caption: text(4096).optional(),
  })
  .strict();
const notice = z
  .object({
    type: z.literal("notice"),
    heading,
    tone: z.enum(["neutral", "positive", "warning", "negative"]),
    text: text(4096),
    attribution: text(256).optional(),
  })
  .strict();

const schema = z
  .object({
    format: z.literal("hartmesh.artifact-view"),
    version: z.literal(1),
    title: text(256, true),
    subtitle: text(512).optional(),
    accent: z
      .string()
      .length(7)
      .regex(/^#[0-9a-fA-F]{6}$/)
      .optional(),
    primary_source: z
      .object({
        path: path.refine(
          (value) => !value.includes("/") && !isArtifactViewPath(value),
        ),
      })
      .strict()
      .optional(),
    destination: z
      .object({ collection: text(64, true) })
      .strict()
      .optional(),
    blocks: z
      .array(
        z.discriminatedUnion("type", [
          facts,
          paragraphs,
          list,
          table,
          image,
          notice,
        ]),
      )
      .max(64),
    exports: z
      .array(z.object({ path, label: text(256, true) }).strict())
      .max(16),
  })
  .strict()
  .superRefine((view, context) => {
    let cells = 0;
    let images = 0;
    for (const block of view.blocks) {
      if (block.type === "image") images += 1;
      if (block.type === "table") {
        const width = block.columns.length;
        if (
          block.rows.some((row) => row.length !== width) ||
          (block.footer && block.footer.length !== width)
        ) {
          context.addIssue({
            code: "custom",
            message: "Table dimensions do not match.",
          });
        }
        cells += width * (1 + block.rows.length + (block.footer ? 1 : 0));
      }
    }
    if (cells > ARTIFACT_VIEW_MAX_CELLS || images > ARTIFACT_VIEW_MAX_IMAGES) {
      context.addIssue({
        code: "custom",
        message: "Document resource budget exceeded.",
      });
    }
    if (
      new Set(view.exports.map((item) => item.path)).size !==
      view.exports.length
    ) {
      context.addIssue({
        code: "custom",
        message: "Export paths must be unique.",
      });
    }
  });

export type ArtifactViewDocument = z.infer<typeof schema>;
export type ArtifactViewBlock = ArtifactViewDocument["blocks"][number];
export type ArtifactViewExport = { path: string; label: string };

/** File discovery is broader than presentation. Only server-owned tags admit exports. */
export function recordedPresentedPaths(
  messages: readonly { type?: string; additional_kwargs?: unknown }[],
): string[] {
  const paths = new Set<string>();
  for (const message of messages) {
    if (message.type !== "ai" && message.type !== "tool") continue;
    const metadata = message.additional_kwargs;
    if (!metadata || typeof metadata !== "object") continue;
    const presented = (metadata as Record<string, unknown>).presented_files;
    if (!Array.isArray(presented)) continue;
    for (const path of presented) if (typeof path === "string") paths.add(path);
  }
  return [...paths];
}

export function parseArtifactView(
  content: string,
): ArtifactViewDocument | null {
  if (
    content.length > ARTIFACT_VIEW_MAX_BYTES ||
    new TextEncoder().encode(content).byteLength > ARTIFACT_VIEW_MAX_BYTES
  )
    return null;
  try {
    const result = schema.safeParse(JSON.parse(content) as unknown);
    return result.success ? result.data : null;
  } catch {
    return null;
  }
}

/** Resolve only literal local references; producer data never supplies an endpoint. */
export function resolveViewReference(
  viewPath: string,
  relative: string,
): string | null {
  if (
    !viewPath.startsWith("/mnt/user-data/") ||
    !isSafeViewReference(viewPath.slice(1)) ||
    !isSafeViewReference(relative)
  )
    return null;
  return viewPath.slice(0, viewPath.lastIndexOf("/") + 1) + relative;
}

export function eligibleViewExports(
  view: ArtifactViewDocument,
  viewPath: string,
  presented: readonly string[],
): ArtifactViewExport[] {
  // Matches the runtime delivery fence: an explicitly presented directory
  // covers named descendants. Never enumerate or recursively copy that path.
  const presentedPaths = presented.map((item) => item.replace(/\/$/, ""));
  return view.exports.flatMap((item) => {
    const path = resolveViewReference(viewPath, item.path);
    return path &&
      presentedPaths.some(
        (item) => path === item || path.startsWith(`${item}/`),
      )
      ? [{ path, label: item.label }]
      : [];
  });
}

export function viewCollection(view: ArtifactViewDocument): {
  collection: string | undefined;
  ignored: boolean;
} {
  const collection = view.destination?.collection;
  if (collection === undefined)
    return { collection: undefined, ignored: false };
  if (
    !isSafeViewReference(collection) ||
    collection.includes("/") ||
    collection.trim() !== collection
  )
    return { collection: undefined, ignored: true };
  return { collection, ignored: false };
}
