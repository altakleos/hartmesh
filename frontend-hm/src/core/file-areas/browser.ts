import { selectLoadedFiles, type FileListOrder } from "./selection";

export type BrowserArea = "personal" | "shared" | "agent";
export type BrowserRow = {
  name: string;
  path: string;
  kind: "directory" | "file" | "symlink" | "unsupported";
  modified: number;
  size: number;
  published_at?: string | null;
  accessible?: boolean;
};

export function browserFolder(raw: string | null): string | null {
  if (!raw) return "";
  if (
    /[\\\p{Cc}]/u.test(raw) ||
    raw.split("/").some((part) => !part || part === "." || part === "..")
  )
    return null;
  return raw;
}

export function browserHref(
  area: BrowserArea,
  folder: string,
  options: { instance?: string; view?: "classic" | "browser" } = {},
) {
  const params = new URLSearchParams();
  const route = area === "agent" ? "/workspace/instances" : "/workspace/files";
  if (area === "agent") {
    if (!options.instance)
      throw new Error("An agent location needs its instance");
    params.set("instance", options.instance);
    params.set("tab", "files");
  } else if (area === "shared") params.set("tab", "shared");
  if (folder && browserFolder(folder) !== null) params.set("folder", folder);
  if (options.view) params.set("view", options.view);
  return route + (params.size ? "?" + params.toString() : "");
}

export function parentFolder(path: string) {
  return path.slice(0, Math.max(0, path.lastIndexOf("/")));
}

export function boundedBrowserRows<T extends BrowserRow>(
  source: readonly T[],
  {
    folder,
    query,
    order,
    locale,
    virtual,
  }: {
    folder: string;
    query: string;
    order: FileListOrder;
    locale: string;
    virtual: boolean;
  },
): BrowserRow[] {
  const children = new Map<string, BrowserRow>();
  const prefix = folder ? folder + "/" : "";
  for (const row of source) {
    if (!row.path || browserFolder(row.path) === null) continue;
    if (query.trim() && virtual) {
      children.set(row.path, row);
      continue;
    }
    if (!row.path.startsWith(prefix)) continue;
    const suffix = row.path.slice(prefix.length);
    const split = suffix.indexOf("/");
    if (split === -1) children.set(row.path, row);
    else if (virtual) {
      const name = suffix.slice(0, split);
      const path = prefix + name;
      if (!children.has(path))
        children.set(path, {
          path,
          name,
          kind: "directory",
          modified: 0,
          size: 0,
        });
    }
  }
  const result = selectLoadedFiles([...children.values()], {
    query,
    order,
    locale,
  });
  if (query.trim()) return result;
  const folders = selectLoadedFiles(
    result.filter((r) => r.kind === "directory"),
    { query: "", order: "name", locale },
  );
  return [...folders, ...result.filter((r) => r.kind !== "directory")];
}
