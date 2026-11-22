import type { Message } from "@langchain/langgraph-sdk";
import { expect, it } from "@rstest/core";

import { importedFileReceipt } from "@/core/messages/imported-file";
import { getMessageGroups } from "@/core/messages/utils";
const receipt = {
  kind: "conversation",
  filename: "plan (2).xlsx",
  source_title: "Budget review",
  size: 17,
  sha256: "a".repeat(64),
  virtual_path: "/mnt/user-data/uploads/plan (2).xlsx",
};
const tool = (data: unknown) =>
  ({
    id: "result",
    type: "tool",
    name: "import_conversation_file",
    tool_call_id: "call",
    content: "Imported a completely different file",
    additional_kwargs: { imported_files: data },
  }) as Message;
it("renders only the typed copied-file receipt, outside the presentation archive", () => {
  expect(importedFileReceipt(tool([receipt]))).toEqual(receipt);
  expect(getMessageGroups([tool([receipt])]).map((g) => g.type)).toContain(
    "assistant:imported-file",
  );
  expect(
    importedFileReceipt(tool([{ ...receipt, source_title: null }]))
      ?.source_title,
  ).toBeNull();
});
it("leaves malformed, unknown, untrusted, or text-only results as ordinary steps", () => {
  for (const value of [
    undefined,
    receipt,
    [],
    [receipt, receipt],
    [{ ...receipt, version: 2 }],
    [{ ...receipt, kind: "new" }],
    [{ ...receipt, source_title: 3 }],
    [{ ...receipt, filename: "../other" }],
    [{ ...receipt, size: -1 }],
    [{ ...receipt, sha256: "A".repeat(64) }],
    [{ ...receipt, virtual_path: "/other" }],
  ]) {
    expect(importedFileReceipt(tool(value))).toBeNull();
    expect(getMessageGroups([tool(value)]).map((g) => g.type)).not.toContain(
      "assistant:imported-file",
    );
  }
  expect(
    importedFileReceipt({ ...tool([receipt]), name: "other" } as Message),
  ).toBeNull();
  expect(
    importedFileReceipt({ ...tool([receipt]), type: "human" } as Message),
  ).toBeNull();
});

it("rejects failed tool results and names that cannot be stored as this gateway's uploads", () => {
  for (const status of ["error", "partial", null])
    expect(
      importedFileReceipt({ ...tool([receipt]), status } as Message),
    ).toBeNull();
  for (const filename of [
    "CON .pdf",
    ".upload-123.part",
    "plan.",
    "a".repeat(256),
  ]) {
    expect(
      importedFileReceipt(
        tool([
          {
            ...receipt,
            filename,
            virtual_path: `/mnt/user-data/uploads/${filename}`,
          },
        ]),
      ),
    ).toBeNull();
  }
});
