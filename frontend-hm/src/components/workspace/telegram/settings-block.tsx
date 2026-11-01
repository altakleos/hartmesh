"use client";

import { useEffect, useRef, useState } from "react";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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

type Field = "locale" | "timezone";

export function TelegramSettingsBlock({ settings }: { settings: Settings }) {
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

  const save = async (field: Field, value: string) => {
    setSaved(null);
    setFailed(null);
    setPending((shown) => ({ ...shown, [field]: value }));
    try {
      await saveTelegramSettings({ [field]: value });
      setSaved(field);
    } catch (error) {
      setFailed({
        field,
        text:
          error instanceof TelegramRefusal &&
          error.reason === "invalid-timezone"
            ? strings.refusal.noTimezone
            : strings.prefs.failed,
      });
    }
    // The control follows the stored value, also after a refusal.
    await refresh();
    setPending((shown) => {
      const rest = { ...shown };
      delete rest[field];
      return rest;
    });
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
      <p className="text-muted-foreground text-xs">{strings.prefs.inChat}</p>
    </section>
  );
}
