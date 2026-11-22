import type { Message } from "@langchain/langgraph-sdk";

import {
  importedAttachment,
  type ImportReceipt,
} from "@/core/threads/conversation-file-import";

interface ConversationImport extends ImportReceipt {
  kind: "conversation";
  source_title: string | null;
}
export function importedFileReceipt(
  message: Message,
): ConversationImport | null {
  if (message.type !== "tool" || message.name !== "import_conversation_file")
    return null;
  if (
    "status" in message &&
    message.status !== undefined &&
    message.status !== "success"
  )
    return null;
  const files = message.additional_kwargs?.imported_files;
  if (!Array.isArray(files) || files.length !== 1) return null;
  const value: unknown = files[0];
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const receipt = value as ConversationImport;
  if (
    Object.keys(value).length !== 6 ||
    receipt.kind !== "conversation" ||
    (receipt.source_title !== null && typeof receipt.source_title !== "string")
  )
    return null;
  try {
    importedAttachment(receipt);
    const leaf = receipt.filename;
    if (
      new TextEncoder().encode(leaf).length > 255 ||
      /[. ]$/.test(leaf) ||
      /^\.upload-.*\.part$/i.test(leaf) ||
      /^(?:con|prn|aux|nul|conin\$|conout\$|(?:com|lpt)[1-9¹²³])$/i.test(
        leaf.split(".")[0]!.trimEnd(),
      )
    )
      return null;
  } catch {
    return null;
  }
  return receipt;
}
