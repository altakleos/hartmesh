export type FileListOrder = "name" | "newest";

type ListedFile = {
  name: string;
  path: string;
  modified: number;
  published_at?: string | null;
};

/** Filter only the returned entries, retaining their original action identities. */
export function selectLoadedFiles<T extends ListedFile>(
  files: readonly T[],
  {
    query,
    order,
    locale,
  }: { query: string; order: FileListOrder; locale: string },
): T[] {
  const normalize = (value: string) =>
    value.normalize("NFKC").toLocaleLowerCase(locale);
  const needle = normalize(query.trim());
  const names = new Intl.Collator(locale, {
    numeric: true,
    sensitivity: "base",
  });
  const byName = (left: T, right: T) =>
    names.compare(left.name, right.name) ||
    names.compare(left.path, right.path) ||
    (left.path < right.path ? -1 : left.path > right.path ? 1 : 0);
  const when = (file: T) => {
    const published = file.published_at ? Date.parse(file.published_at) : NaN;
    const modified = file.modified * 1000;
    return Number.isFinite(published)
      ? published
      : Number.isFinite(modified)
        ? modified
        : 0;
  };
  return files
    .filter(
      (file) =>
        normalize(file.name).includes(needle) ||
        normalize(file.path).includes(needle),
    )
    .sort((left, right) =>
      order === "newest"
        ? when(right) - when(left) || byName(left, right)
        : byName(left, right),
    );
}
