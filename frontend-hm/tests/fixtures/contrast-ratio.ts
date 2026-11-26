import type { Locator } from "@playwright/test";

export async function contrastRatio(locator: Locator) {
  return locator.evaluate((element) => {
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = 1;
    const ctx = canvas.getContext("2d")!;
    const rgba = (color: string) => {
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillStyle = color;
      ctx.fillRect(0, 0, 1, 1);
      return Array.from(ctx.getImageData(0, 0, 1, 1).data);
    };
    const luminance = (values: number[]) =>
      values
        .slice(0, 3)
        .map((v) => {
          const c = v / 255;
          return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
        })
        .reduce((n, c, i) => n + c * [0.2126, 0.7152, 0.0722][i]!, 0);
    const fg = luminance(rgba(getComputedStyle(element).color));
    let parent: Element | null = element;
    let bg = [255, 255, 255, 255];
    while (parent) {
      const value = rgba(getComputedStyle(parent).backgroundColor);
      if (value[3] === 255) {
        bg = value;
        break;
      }
      parent = parent.parentElement;
    }
    const b = luminance(bg);
    return (Math.max(fg, b) + 0.05) / (Math.min(fg, b) + 0.05);
  });
}
