import { expect, test } from "@rstest/core";

import { DEFAULT_PRODUCT_NAME, productNameFrom } from "@/core/product";

test("the product is called HartMesh unless a deployment names it", () => {
  expect(DEFAULT_PRODUCT_NAME).toBe("HartMesh");
});

test("a Gateway answer supplies the name", () => {
  expect(productNameFrom({ name: "Acme Assist" })).toBe("Acme Assist");
});

test("a body that is not the answer leaves the default", () => {
  for (const body of [
    null,
    undefined,
    "Acme Assist",
    ["Acme Assist"],
    {},
    { name: 42 },
    { name: "" },
    { name: "   " },
    { detail: "Bad Gateway" },
  ]) {
    expect(productNameFrom(body)).toBe(DEFAULT_PRODUCT_NAME);
  }
});
