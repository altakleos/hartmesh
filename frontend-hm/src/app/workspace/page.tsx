import { redirect } from "next/navigation";

import { DEMO_THREAD_IDS } from "@/core/threads/static-demo";
import { env } from "@/env";

/** The query goes along: bots and the sign-in return address this page with one. */
export default async function WorkspacePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(await searchParams)) {
    for (const item of [value ?? []].flat()) {
      query.append(key, item);
    }
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  if (env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true") {
    return redirect(`/workspace/chats/${DEMO_THREAD_IDS[0]}${suffix}`);
  }
  return redirect(`/workspace/chats/new${suffix}`);
}
