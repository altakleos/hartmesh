import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";

rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
const i18n = rs.hoisted(() => ({ t: {} as unknown }));
rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: i18n.t, locale: "en-US" }),
}));
const selects = rs.hoisted(() => ({
  change: new Map<string, (value: string) => void>(),
}));
rs.mock("@/components/ui/select", () => ({
  Select: ({
    value,
    onValueChange,
    children,
  }: {
    value: string;
    onValueChange: (value: string) => void;
    children: ReactNode;
  }) => {
    selects.change.set(value, onValueChange);
    return <output data-value={value}>{children}</output>;
  },
  SelectTrigger: () => null,
  SelectValue: () => null,
  SelectContent: () => null,
  SelectItem: () => null,
}));

import {
  TelegramSettingsBlock,
  timeZonesWith,
} from "@/components/workspace/telegram/settings-block";
import { fetch } from "@/core/api/fetcher";
import { createEnUS } from "@/core/i18n/locales/en-US";
import type { Settings } from "@/core/telegram-bots/api";

import { clearClients, json, renderWithClient, server } from "./support";

i18n.t = createEnUS("HartMesh");

const settings: Settings = {
  timezone: "Europe/Lisbon",
  locale: "en-US",
  quiet_enabled: true,
  quiet_start: "21:00",
  quiet_end: "07:00",
  notify: "all",
  summary_enabled: true,
  summary_time: "08:00",
};

function start(answer: () => Response) {
  const { calls, handle } = server({
    "PUT /api/telegram-bots/settings": answer,
  });
  rs.mocked(fetch).mockImplementation(handle);
  renderWithClient(<TelegramSettingsBlock settings={settings} />);
  return calls;
}

afterEach(() => {
  cleanup();
  clearClients();
  selects.change.clear();
  rs.mocked(fetch).mockReset();
});

const shown = () =>
  Array.from(document.querySelectorAll("output")).map((element) =>
    element.getAttribute("data-value"),
  );

describe("the settings block", () => {
  it("language and time zone are saved on change", async () => {
    const calls = start(() => json(settings));
    selects.change.get("en-US")?.("zh-CN");
    await screen.findByText("Saved.");
    selects.change.get("Europe/Lisbon")?.("Asia/Tokyo");
    await waitFor(() => expect(calls).toHaveLength(2));
    expect(calls.map(({ body }) => body)).toEqual([
      { locale: "zh-CN" },
      { timezone: "Asia/Tokyo" },
    ]);
  });

  it("a refusal restores the stored value and shows its sentence", async () => {
    start(() => json({ detail: "no", reason: "invalid-timezone" }, 400));
    selects.change.get("Europe/Lisbon")?.("Asia/Tokyo");
    await screen.findByText("Choose your time zone, then try again.");
    await waitFor(() => expect(shown()).toContain("Europe/Lisbon"));
  });

  it("any other refusal shows the general sentence", async () => {
    start(() => json({ detail: "no", reason: "something-new" }, 400));
    selects.change.get("en-US")?.("zh-CN");
    await screen.findByText("That could not be saved. Try again.");
    await waitFor(() => expect(shown()).toContain("en-US"));
  });

  it("keeps showing a choice while its own request runs", async () => {
    const answers: ((response: Response) => void)[] = [];
    start(() => json(settings));
    rs.mocked(fetch).mockImplementation(
      () => new Promise((resolve) => answers.push(resolve)),
    );
    selects.change.get("en-US")?.("zh-CN");
    selects.change.get("Europe/Lisbon")?.("Asia/Tokyo");
    await waitFor(() => expect(answers).toHaveLength(2));
    await waitFor(() => expect(shown()).toContain("Asia/Tokyo"));
    answers[0]?.(json(settings));
    await waitFor(() => expect(shown()).toContain("en-US"));
    expect(shown()).toContain("Asia/Tokyo");
    answers[1]?.(json(settings));
    await waitFor(() => expect(shown()).toContain("Europe/Lisbon"));
  });

  it("offers the stored zone in a browser that lists none", () => {
    const list = Intl.supportedValuesOf;
    Reflect.set(Intl, "supportedValuesOf", undefined);
    expect(timeZonesWith("Asia/Tokyo")).toEqual(["Asia/Tokyo"]);
    Reflect.set(Intl, "supportedValuesOf", list);
  });

  it("keeps a stored zone this browser does not list", () => {
    expect(timeZonesWith("Old/Zone")[0]).toBe("Old/Zone");
    expect(timeZonesWith("Asia/Tokyo")).toContain("Asia/Tokyo");
  });
});
