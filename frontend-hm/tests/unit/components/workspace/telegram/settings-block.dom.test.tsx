import { afterEach, describe, expect, it, rs } from "@rstest/core";
import {
  act,
  cleanup,
  fireEvent,
  screen,
  waitFor,
} from "@testing-library/react";
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
  renderWithClient(
    <TelegramSettingsBlock settings={settings} showMessageSettings />,
  );
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

describe("message preferences", () => {
  it("saves notification and both switches using only the changed field", async () => {
    const calls = start(() => json(settings));
    selects.change.get("all")?.("questions");
    await waitFor(() => expect(calls).toHaveLength(1));
    fireEvent.click(screen.getByRole("switch", { name: "Quiet hours" }));
    await waitFor(() => expect(calls).toHaveLength(2));
    fireEvent.click(screen.getByRole("switch", { name: "Morning summary" }));
    await waitFor(() => expect(calls).toHaveLength(3));
    expect(calls.map(({ body }) => body)).toEqual([
      { notify: "questions" },
      { quiet_enabled: false },
      { summary_enabled: false },
    ]);
  });

  it("saves a time on blur and refuses equal quiet times without a request", async () => {
    const calls = start(() => json(settings));
    const from = screen.getByLabelText("From");
    fireEvent.change(from, { target: { value: "07:00" } });
    fireEvent.blur(from);
    await screen.findByText("The two times must differ.");
    expect(calls).toHaveLength(0);
    fireEvent.change(from, { target: { value: "22:30" } });
    expect(calls).toHaveLength(0);
    fireEvent.blur(from);
    await waitFor(() =>
      expect(calls.map(({ body }) => body)).toEqual([{ quiet_start: "22:30" }]),
    );
  });

  it("restores a refused time and shows the fixed invalid-time sentence", async () => {
    start(() => json({ reason: "invalid-time" }, 400));
    const at = screen.getByLabelText<HTMLInputElement>("At");
    fireEvent.change(at, { target: { value: "09:15" } });
    fireEvent.blur(at);
    await screen.findByText("Use a time like 21:00.");
    await waitFor(() => expect(at.value).toBe("08:00"));
  });

  it("hides proactive settings when the caller disables them", () => {
    renderWithClient(
      <TelegramSettingsBlock settings={settings} showMessageSettings={false} />,
    );
    expect(screen.queryByRole("switch")).toBeNull();
    expect(selects.change.has("all")).toBe(false);
  });
});

it("retires an admitted settings save with its account-owned block", async () => {
  let release!: (response: Response) => void;
  let signal: AbortSignal | null | undefined;
  rs.mocked(fetch).mockImplementation((_url, init) => {
    signal = init?.signal;
    return new Promise((resolve) => {
      release = resolve;
    });
  });
  const view = renderWithClient(
    <TelegramSettingsBlock settings={settings} showMessageSettings />,
  );
  selects.change.get("all")?.("none");
  await waitFor(() => expect(signal).toBeTruthy());
  view.unmount();
  expect(signal?.aborted).toBe(true);
  renderWithClient(
    <TelegramSettingsBlock settings={settings} showMessageSettings />,
  );
  await act(async () => {
    release(json({ ...settings, notify: "none" }));
  });
  expect(shown()).toContain("all");
  expect(screen.queryByText("Saved.")).toBeNull();
});
