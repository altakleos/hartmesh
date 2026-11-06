import { z } from "zod";

import { DEFAULT_PRODUCT_NAME } from ".";

const boundedName = z
  .string()
  .min(1)
  .max(80)
  .refine((v) => Array.from(v).length <= 40 && v === v.trim());
const name = boundedName.refine(
  (v) => !/[\p{Cc}\p{Cf}\p{Cs}\u2028\u2029]/u.test(v),
);
// Match the established UiConfig policy in unconfigured mode, including valid
// joiners. The strict mounted payload has its own explicitly narrower policy.
const legacyName = boundedName.refine(
  (v) =>
    !/[\u0000-\u001f\u007f\u202a-\u202e\u2066-\u2069\u200b\u200e\u200f\ufeff\p{Cs}]/u.test(
      v,
    ),
);
const png = z
  .string()
  .max(350_000)
  .regex(/^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/);
const support = z
  .string()
  .max(4096)
  .url()
  .refine((value) => {
    const u = new URL(value);
    return (
      Array.from(value).length <= 2048 &&
      u.protocol === "https:" &&
      !u.username &&
      !u.password &&
      !/[\s\\\p{Cc}\p{Cf}\p{Cs}]/u.test(value) &&
      !/[@%]/.test(u.host)
    );
  });
const identity = z.discriminatedUnion("mode", [
  z
    .object({
      schema_version: z.literal(1),
      mode: z.literal("configured"),
      name,
      product: z.object({ name, logo: png, browser_icon: png }).strict(),
      provider: z.object({ name, logo: png, support_url: support }).strict(),
    })
    .strict(),
  z
    .object({
      schema_version: z.literal(1),
      mode: z.literal("default"),
      name: legacyName,
      product: z
        .object({ name: legacyName, logo: z.null(), browser_icon: z.null() })
        .strict(),
      provider: z.null(),
    })
    .strict(),
]);
export type ProductIdentity = z.infer<typeof identity>;
export const DEFAULT_IDENTITY: ProductIdentity = {
  schema_version: 1,
  mode: "default",
  name: DEFAULT_PRODUCT_NAME,
  product: { name: DEFAULT_PRODUCT_NAME, logo: null, browser_icon: null },
  provider: null,
};
export const IDENTITY_UNAVAILABLE = "Service identity unavailable";
export function parseProductIdentity(body: unknown): ProductIdentity | null {
  const result = identity.safeParse(body);
  return result.success && result.data.name === result.data.product.name
    ? result.data
    : null;
}
