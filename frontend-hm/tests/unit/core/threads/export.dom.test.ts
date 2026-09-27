import { afterEach, describe, expect, it, rs } from "@rstest/core";

import { exportThread } from "@/core/threads/export";

describe("exportThread in the browser", () => {
  afterEach(() => {
    rs.unstubAllGlobals();
    rs.restoreAllMocks();
  });

  it("saves the Gateway's file under the name it gave", async () => {
    const fetchMock = rs.fn(
      async () =>
        new Response("# Monthly review\n", {
          status: 200,
          headers: {
            "Content-Disposition":
              "attachment; filename=\"conversation.md\"; filename*=UTF-8''%E6%9C%88%E5%BA%A6%20review.md",
          },
        }),
    );
    rs.stubGlobal("fetch", fetchMock);
    const saved: { name: string; href: string }[] = [];
    rs.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      saved.push({ name: this.download, href: this.href });
    });
    const created = rs
      .spyOn(URL, "createObjectURL")
      .mockReturnValue("blob:transcript");
    const revoked = rs
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => undefined);

    await exportThread("thread-1", "markdown");

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(saved).toEqual([
      { name: "月度 review.md", href: "blob:transcript" },
    ]);
    const blob = created.mock.calls[0]![0] as Blob;
    expect(await blob.text()).toBe("# Monthly review\n");
    expect(revoked).toHaveBeenCalledWith("blob:transcript");
  });
});
