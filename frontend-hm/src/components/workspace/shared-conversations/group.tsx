"use client";
import Link from "next/link";

import { Button } from "@/components/ui/button";
import { snapshotHref, SnapshotError } from "@/core/shared-conversations/api";
import type { useSnapshotList } from "@/core/shared-conversations/hooks";

import { useSnapshotCopy } from "./copy";

export function ConversationGroup({
  listing,
  filter = "",
}: {
  listing: ReturnType<typeof useSnapshotList>;
  filter?: string;
}) {
  const w = useSnapshotCopy();
  if (listing.isError) {
    if (listing.error instanceof SnapshotError && listing.error.status === 404)
      return null;
    return (
      <section aria-label={w.group}>
        <p role="alert">{w.readError}</p>
        <Button variant="outline" onClick={() => void listing.refetch()}>
          {w.retry}
        </Button>
      </section>
    );
  }
  if (
    !listing.data ||
    (!listing.data.conversations.length && !listing.hasNextPage)
  )
    return null;
  const query = filter.toLocaleLowerCase(w.locale).trim();
  const rows = listing.data.conversations.filter((row) =>
    row.title.toLocaleLowerCase(w.locale).includes(query),
  );
  if (!rows.length && !listing.hasNextPage) return null;
  return (
    <section className="my-4 space-y-2" aria-label={w.group}>
      <h2 className="font-semibold">{w.group}</h2>
      <ul className="divide-y rounded-md border">
        {rows.map((row) => (
          <li className="px-3 py-2" key={row.publication_id}>
            <Link
              className="font-medium underline"
              href={snapshotHref(row.publication_id)}
            >
              {row.title || w.untitled}
            </Link>
            <p className="text-muted-foreground text-sm">
              {w.by(
                row.publisher ?? w.unknown,
                new Date(row.published_at).toLocaleDateString(w.locale),
              )}
            </p>
          </li>
        ))}
      </ul>
      {listing.data.truncated && <p role="status">{w.partial}</p>}
      {listing.hasNextPage && (
        <Button
          variant="outline"
          disabled={listing.isFetchingNextPage}
          onClick={() => void listing.fetchNextPage()}
        >
          {w.more}
        </Button>
      )}
    </section>
  );
}
