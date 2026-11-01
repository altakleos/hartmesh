import { afterEach, describe, expect, it } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

import { TelegramQrCode } from "@/components/workspace/telegram/qr-code";

afterEach(cleanup);

describe("TelegramQrCode", () => {
  it("draws an inline code of the design's size, named by the line under it", () => {
    render(<TelegramQrCode value="https://t.me/x?start=code" label="Scan" />);
    const code = screen.getByRole("img", { name: "Scan" });
    expect(code.tagName.toLowerCase()).toBe("svg");
    expect(code.getAttribute("width")).toBe("176");
    expect(code.getAttribute("height")).toBe("176");
    expect(document.querySelector("img")).toBeNull();
    screen.getByText("Scan");
  });
});
