/** What the product is called when the deployment names nothing (`ui.product_name`). */
export const DEFAULT_PRODUCT_NAME = "HartMesh";

/**
 * The name out of a `GET /api/product` body, or the default. The Gateway has
 * already validated it; this only refuses a body that is not the answer, so a
 * proxy's error page never becomes the product's name.
 */
export function productNameFrom(body: unknown): string {
  if (typeof body === "object" && body !== null && "name" in body) {
    const { name } = body as { name: unknown };
    if (typeof name === "string" && name.trim().length > 0) {
      return name.trim();
    }
  }
  return DEFAULT_PRODUCT_NAME;
}
