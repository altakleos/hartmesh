import { expect, test } from "@playwright/test";

/**
 * The default E2E config runs with auth disabled, so it cannot see this: the
 * standalone artifact window is behind the session for every target. A bundled
 * demo file with `mock=true` is no exception; the public showcase it served is
 * gone.
 */

// A file bundled with the static demo — see STATIC_DEMO_ARTIFACTS.
const DEMO_THREAD_ID = "3823e443-4e2b-4679-b496-a9506eae462b";
const DEMO_ARTIFACT = "/mnt/user-data/outputs/fei-fei-li-podcast-timeline.md";

function viewerUrl(params: Record<string, string>) {
  return `/artifacts/view?${new URLSearchParams(params).toString()}`;
}

test.describe("standalone artifact viewer access", () => {
  const targets: Record<string, string>[] = [
    { path: DEMO_ARTIFACT, thread_id: DEMO_THREAD_ID, mock: "true" },
    { path: DEMO_ARTIFACT, thread_id: DEMO_THREAD_ID },
  ];
  for (const params of targets) {
    test(`sends a logged-out visitor to login (mock=${params.mock ?? "absent"})`, async ({
      page,
    }) => {
      await page.goto(viewerUrl(params));

      await expect(page).toHaveURL(/\/login\?next=/, { timeout: 15_000 });
    });
  }
});
