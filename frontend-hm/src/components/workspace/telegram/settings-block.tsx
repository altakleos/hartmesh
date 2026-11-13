"use client";

import { memo, useEffect, useRef, useState } from "react";

import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { useI18n } from "@/core/i18n/hooks";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";
import {
  saveTelegramSettings,
  type Settings,
  TelegramRefusal,
} from "@/core/telegram-bots/api";
import { useRefreshTelegramBots } from "@/core/telegram-bots/hooks";

import {
  settleTelegramSettingsFocus,
  useTelegramPageState,
} from "./setup-dialog-store";

const LANGUAGES = [
  { value: "en-US", label: enUS.locale.localName },
  { value: "zh-CN", label: zhCN.locale.localName },
];

/** Every zone the browser knows, and the stored one even if it knows no such zone. */
export function timeZonesWith(current: string): string[] {
  const zones =
    typeof Intl.supportedValuesOf === "function"
      ? Intl.supportedValuesOf("timeZone")
      : [];
  return zones.includes(current) ? zones : [current, ...zones];
}

type Field = keyof Settings;
type TimeField = "quiet_start" | "quiet_end" | "summary_time";

/** Memoised because it draws every time zone and the section's clock ticks around it. */
export const TelegramSettingsBlock = memo(function TelegramSettingsBlock({
  settings,
  showMessageSettings = false,
}: {
  settings: Settings;
  showMessageSettings?: boolean;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const refresh = useRefreshTelegramBots();
  const [saved, setSaved] = useState<Field | null>(null);
  const [failed, setFailed] = useState<{ field: Field; text: string } | null>(
    null,
  );
  const [pending, setPending] = useState<Partial<Settings>>({});
  const { focusSettings } = useTelegramPageState();
  const block = useRef<HTMLElement>(null);
  const lifetime = useRef(new AbortController());
  const busy = useRef(new Set<Field>());
  const [drafts, setDrafts] = useState<Partial<Record<TimeField, string>>>({});
  useEffect(() => {
    const owner = new AbortController();
    lifetime.current = owner;
    return () => owner.abort();
  }, []);

  // After the dialog that asked closes, as it gives the focus back when it does.
  useEffect(() => {
    if (!focusSettings) {
      return;
    }
    const timer = setTimeout(() => {
      block.current?.focus();
      block.current?.scrollIntoView();
      settleTelegramSettingsFocus();
    });
    return () => clearTimeout(timer);
  }, [focusSettings]);

  const save = async (field: Field, value: string | boolean) => {
    const signal = lifetime.current.signal;
    if (signal.aborted || busy.current.has(field)) return;
    busy.current.add(field);
    setSaved(null);
    setFailed(null);
    setPending((shown) => ({ ...shown, [field]: value }));
    try {
      await saveTelegramSettings({ [field]: value }, signal);
      if (signal.aborted) return;
      setSaved(field);
    } catch (error) {
      if (signal.aborted) return;
      setFailed({
        field,
        text:
          error instanceof TelegramRefusal &&
          error.reason === "invalid-timezone"
            ? strings.refusal.noTimezone
            : error instanceof TelegramRefusal &&
                error.reason === "invalid-time"
              ? strings.prefs.badTime
              : strings.prefs.failed,
      });
    }
    await refresh();
    if (signal.aborted) return;
    busy.current.delete(field);
    setPending((shown) => {
      const rest = { ...shown };
      delete rest[field];
      return rest;
    });
    setDrafts((shown) => {
      const rest = { ...shown };
      delete rest[field as TimeField];
      return rest;
    });
  };

  const saveTime = (field: TimeField, value: string) => {
    setSaved(null);
    if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(value)) {
      setFailed({ field, text: strings.prefs.badTime });
      return;
    }
    if (
      field !== "summary_time" &&
      value ===
        (field === "quiet_start" ? settings.quiet_end : settings.quiet_start)
    ) {
      setFailed({ field, text: strings.prefs.quiet.same });
      return;
    }
    if (value !== settings[field]) void save(field, value);
  };

  const result = (field: Field) =>
    failed?.field === field ? (
      <p className="text-destructive text-xs" role="alert">
        {failed.text}
      </p>
    ) : saved === field ? (
      <p className="text-muted-foreground text-xs" role="status">
        {strings.prefs.saved}
      </p>
    ) : null;

  const timeField = (field: TimeField, label: string) => (
    <div className="space-y-1">
      <label htmlFor={`telegram-${field}`} className="text-sm">
        {label}
      </label>
      <Input
        id={`telegram-${field}`}
        type="time"
        value={drafts[field] ?? pending[field] ?? settings[field]}
        disabled={
          pending[field] !== undefined ||
          (field !== "summary_time" &&
            (pending.quiet_start !== undefined ||
              pending.quiet_end !== undefined))
        }
        onChange={(event) =>
          setDrafts((shown) => ({ ...shown, [field]: event.target.value }))
        }
        onBlur={(event) => saveTime(field, event.currentTarget.value)}
      />
      {result(field)}
    </div>
  );

  return (
    <section ref={block} className="space-y-4" tabIndex={-1}>
      <h3 className="font-medium">{strings.prefs.title}</h3>
      <div className="space-y-1">
        <div className="text-sm font-medium" id="telegram-language">
          {strings.prefs.language.label}
        </div>
        <p className="text-muted-foreground text-xs">
          {strings.prefs.language.body}
        </p>
        <Select
          value={pending.locale ?? settings.locale}
          disabled={pending.locale !== undefined}
          onValueChange={(value) => void save("locale", value)}
        >
          <SelectTrigger aria-labelledby="telegram-language">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LANGUAGES.map((language) => (
              <SelectItem key={language.value} value={language.value}>
                {language.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {result("locale")}
      </div>
      <div className="space-y-1">
        <div className="text-sm font-medium" id="telegram-timezone">
          {strings.prefs.timezone.label}
        </div>
        <p className="text-muted-foreground text-xs">
          {strings.prefs.timezone.body}
        </p>
        <Select
          value={pending.timezone ?? settings.timezone}
          disabled={pending.timezone !== undefined}
          onValueChange={(value) => void save("timezone", value)}
        >
          <SelectTrigger aria-labelledby="telegram-timezone">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {timeZonesWith(settings.timezone).map((zone) => (
              <SelectItem key={zone} value={zone}>
                {zone}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {result("timezone")}
      </div>
      {showMessageSettings ? (
        <>
          <p className="text-muted-foreground text-sm">{strings.prefs.intro}</p>
          <div className="space-y-1">
            <div id="telegram-notify" className="text-sm font-medium">
              {strings.prefs.notify.label}
            </div>
            <Select
              value={pending.notify ?? settings.notify}
              disabled={pending.notify !== undefined}
              onValueChange={(value) => void save("notify", value)}
            >
              <SelectTrigger aria-labelledby="telegram-notify">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(["all", "questions", "none"] as const).map((value) => (
                  <SelectItem key={value} value={value}>
                    {strings.prefs.notify[value]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {result("notify")}
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-4">
              <label htmlFor="telegram-quiet" className="text-sm font-medium">
                {strings.prefs.quiet.label}
              </label>
              <Switch
                id="telegram-quiet"
                checked={pending.quiet_enabled ?? settings.quiet_enabled}
                disabled={pending.quiet_enabled !== undefined}
                onCheckedChange={(value) => void save("quiet_enabled", value)}
              />
            </div>
            {result("quiet_enabled")}
            {(pending.quiet_enabled ?? settings.quiet_enabled) ? (
              <>
                <p className="text-muted-foreground text-xs">
                  {strings.prefs.quiet.body({
                    start: settings.quiet_start,
                    end: settings.quiet_end,
                  })}
                </p>
                <div className="grid grid-cols-2 gap-3">
                  {timeField("quiet_start", strings.prefs.quiet.from)}
                  {timeField("quiet_end", strings.prefs.quiet.to)}
                </div>
              </>
            ) : null}
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-4">
              <label htmlFor="telegram-summary" className="text-sm font-medium">
                {strings.prefs.summary.label}
              </label>
              <Switch
                id="telegram-summary"
                checked={pending.summary_enabled ?? settings.summary_enabled}
                disabled={pending.summary_enabled !== undefined}
                onCheckedChange={(value) => void save("summary_enabled", value)}
              />
            </div>
            {result("summary_enabled")}
            {(pending.summary_enabled ?? settings.summary_enabled) ? (
              <>
                <p className="text-muted-foreground text-xs">
                  {strings.prefs.summary.body({ time: settings.summary_time })}
                </p>
                {timeField("summary_time", strings.prefs.summary.at)}
              </>
            ) : null}
          </div>
          <p className="text-muted-foreground text-xs">{strings.prefs.mute}</p>
        </>
      ) : null}
      <p className="text-muted-foreground text-xs">{strings.prefs.inChat}</p>
    </section>
  );
});
