import { expect, test } from "@playwright/test";

import {
  agent,
  AGENT_ID,
  assignment,
  HOME_ID,
  refreshAccount,
  setupAgents,
  WORK_ID,
} from "../e2e-real-backend/utils/compose-agents";
import { inputRequest } from "../fixtures/attention";

for (const surface of ["Work", "Attention"] as const) {
  test(`${surface} opens the selected evidence file under the reviewer's current grant`, async ({
    page,
  }) => {
    const state = await setupAgents(page, {
      instance: agent({ permissions: 2 }),
      role: "user",
    });
    await page.route("**/api/v1/auth/me", (route) =>
      route.fulfill({ json: { ...state.user, permissions: ["agents:read"] } }),
    );
    state.feature.ui = { profile: "business" };
    const space = surface === "Work" ? HOME_ID : "f".repeat(32);
    const paths = [
      "review/round 1/first #1.txt",
      "review/round 1/second & final.txt",
    ];
    const sources = paths.map((path) => ({
      kind: "space_file" as const,
      space_id: space,
      path,
    }));
    const projected = [...sources, { kind: "unavailable" as const }];
    state.work = [assignment({ sources: projected })];
    const request = inputRequest({
      id: "e".repeat(32),
      instance_id: AGENT_ID,
      recipient_id: state.user.id,
      sources: projected,
    });
    state.humanInputs = [request];
    let granted = true;
    await page.route(`**/api/spaces/${space}/content?**`, (route) => {
      const params = new URL(route.request().url()).searchParams;
      const index = paths.indexOf(params.get("path") ?? "");
      if (index < 0 || params.has("download") || !granted)
        return route.fulfill({
          status: 404,
          contentType: "text/plain",
          body: "File unavailable",
        });
      return route.fulfill({
        contentType: "text/plain",
        body: `Review document ${index + 1}`,
      });
    });
    const entry =
      surface === "Work"
        ? `/workspace/instances?instance=${AGENT_ID}&tab=work&work=${WORK_ID}`
        : `/workspace/attention?request=${request.id}`;
    for (const [index, path] of [...paths, paths[0]!].entries()) {
      await page.goto(entry);
      const name =
        surface === "Work"
          ? path
          : `${path} · Current file contents are unchecked.`;
      const link = page.getByRole("link", { name, exact: true });
      await expect(link).toBeVisible();
      await refreshAccount(page);
      await expect(link).toBeVisible();
      // Keep the displayed reference while withdrawing the grant on the third open.
      granted = index < paths.length;
      await link.click();
      await expect(page.locator("body")).toHaveText(
        granted ? `Review document ${index + 1}` : "File unavailable",
      );
    }
  });
}
