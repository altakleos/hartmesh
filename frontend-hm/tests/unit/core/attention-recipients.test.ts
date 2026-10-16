import { expect, test } from "@rstest/core";

import { eligibleInputRecipient } from "@/core/attention/recipients";

test.each([0, 1, 2, 4, 5])(
  "mask %s cannot answer information requests",
  (mask) => expect(eligibleInputRecipient(mask, "information")).toBe(false),
);
test.each([3, 7])("mask %s can answer information requests", (mask) =>
  expect(eligibleInputRecipient(mask, "information")).toBe(true),
);
test.each(["decision", "review"] as const)(
  "only Inspect+Manage members answer %s",
  (purpose) => {
    for (const mask of [0, 1, 2, 3, 4, 5])
      expect(eligibleInputRecipient(mask, purpose)).toBe(false);
    for (const mask of [6, 7])
      expect(eligibleInputRecipient(mask, purpose)).toBe(true);
  },
);
