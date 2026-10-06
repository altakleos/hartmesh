import { describe, expect, it, rs } from "@rstest/core";

import type { FileCollectionContext } from "@/core/extensions/contracts";
import {
  installedFileCollection,
  isFileFilingReady,
} from "@/core/extensions/filing";
import type { LoadedContribution } from "@/core/extensions/registry";

const context = {
  filepath: "/mnt/user-data/outputs/result.pdf",
  destination: "shared" as const,
  presented: ["/mnt/user-data/outputs/result.pdf"],
};

function entry(
  callback: (input: FileCollectionContext) => { collection: string } | null,
  enabled = true,
): LoadedContribution {
  return {
    namespace: "example.files",
    module: "files.v1",
    entry: "installed-entry",
    title: "Files",
    description: "",
    settings: { enabled },
    extension: {
      apiVersion: 1,
      module: "files.v1",
      fileFilingApiVersion: 1,
      fileCollection: callback,
    },
  };
}

describe("installed file collection", () => {
  it("observes an invalid asynchronous callback rejection before falling back", async () => {
    const rejected = Promise.reject(new Error("Broken filing callback"));
    const observed = rs.spyOn(rejected, "catch");
    const callback = (() => rejected) as unknown as Parameters<typeof entry>[0];
    try {
      expect(
        installedFileCollection([entry(callback)], context),
      ).toBeUndefined();
      expect(observed).toHaveBeenCalledTimes(1);
    } finally {
      await rejected.catch(() => undefined);
    }
  });
  it("waits for discovery and module failures instead of silently changing filing", () => {
    expect(isFileFilingReady({ isPending: true })).toBe(false);
    expect(isFileFilingReady({ isError: true })).toBe(false);
    expect(
      isFileFilingReady({
        data: [
          { ...entry(() => null), extension: undefined, error: "Unavailable" },
        ],
      }),
    ).toBe(false);
    expect(isFileFilingReady({ data: [] })).toBe(true);
    expect(isFileFilingReady({ data: [entry(() => null, false)] })).toBe(true);
  });
  it("uses only an enabled installed callback and leaves ordinary files at root", () => {
    const callback = rs.fn(() => ({ collection: "Documents" }));
    expect(installedFileCollection([entry(callback)], context)).toBe(
      "Documents",
    );
    expect(callback).toHaveBeenCalledWith(context);
    callback.mockClear();
    expect(
      installedFileCollection([entry(callback, false)], context),
    ).toBeUndefined();
    expect(callback).not.toHaveBeenCalled();
    expect(installedFileCollection([], context)).toBeUndefined();
    expect(
      installedFileCollection([entry(() => null)], context),
    ).toBeUndefined();
  });

  it.each([
    "../escape",
    "nested/folder",
    " padded ",
    "",
    "https://example.test",
  ])("refuses an invalid destination %s", (collection) => {
    expect(
      installedFileCollection([entry(() => ({ collection }))], context),
    ).toBeUndefined();
  });

  it("contains failures and refuses conflicting installed decisions", () => {
    expect(
      installedFileCollection(
        [
          entry(() => {
            throw new Error("unavailable");
          }),
        ],
        context,
      ),
    ).toBeUndefined();
    expect(
      installedFileCollection(
        [
          entry(() => ({ collection: "One" })),
          {
            ...entry(() => ({ collection: "Two" })),
            namespace: "example.other",
          },
        ],
        context,
      ),
    ).toBeUndefined();
  });

  it("gives callbacks immutable snapshots instead of mutable caller data", () => {
    installedFileCollection(
      [
        entry((input) => {
          expect(Object.isFrozen(input)).toBe(true);
          expect(Object.isFrozen(input.presented)).toBe(true);
          return null;
        }),
      ],
      context,
    );
    expect(context.presented).toEqual(["/mnt/user-data/outputs/result.pdf"]);
  });
});
