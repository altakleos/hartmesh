import { expect, test } from "@rstest/core";

import {
  importedAttachment,
  nextPresentedCursor,
} from "@/core/threads/conversation-file-import";

const digest = "a".repeat(64);
test("accepts only a complete receipt for a direct child of destination uploads", () => {
  const receipt = {
    virtual_path: "/mnt/user-data/uploads/notes_1.txt",
    filename: "notes_1.txt",
    size: 11,
    sha256: digest,
  };
  expect(importedAttachment(receipt)).toEqual({
    filename: "notes_1.txt",
    path: receipt.virtual_path,
    size: 11,
    status: "uploaded",
  });
  for (const delta of [
    { sha256: "" },
    { filename: "notes.txt" },
    { size: -1 },
    { virtual_path: "/mnt/user-data/outputs/notes_1.txt" },
    { virtual_path: "/mnt/user-data/uploads/sub/notes_1.txt" },
  ]) {
    expect(() => importedAttachment({ ...receipt, ...delta })).toThrow();
  }
});
test("an empty presented-files page with a paired cursor can advance", () => {
  expect(
    nextPresentedCursor({
      files: [],
      has_more: true,
      next_before_created_at: "2026-01-01T00:00:00Z",
      next_before_run_id: "completed-run",
    }),
  ).toEqual({
    before_created_at: "2026-01-01T00:00:00Z",
    before_run_id: "completed-run",
  });
  expect(() =>
    nextPresentedCursor({
      files: [],
      has_more: true,
      next_before_created_at: null,
      next_before_run_id: "completed-run",
    }),
  ).toThrow();
});
