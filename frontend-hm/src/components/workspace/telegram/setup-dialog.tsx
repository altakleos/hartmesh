"use client";

import { useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useAgents } from "@/core/agents/hooks";
import {
  closeConnectWindow,
  openConnectUrl,
  prepareConnectWindow,
} from "@/core/channels/open-connect-url";
import { useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";
import {
  addTelegramBot,
  type Bot,
  type Connect,
  disconnectTelegramBot,
  requestTelegramConnect,
  TELEGRAM_SECTION_REFUSALS,
  TelegramRefusal,
  telegramRefusalKey,
} from "@/core/telegram-bots/api";
import {
  useRefreshTelegramBots,
  useTelegramBots,
  useTelegramClock,
  useTelegramReader,
} from "@/core/telegram-bots/hooks";
import {
  type RowLine,
  rowOf,
  sectionNoticeOf,
  setupStageOf,
  stopsSetup,
  targetRowsOf,
} from "@/core/telegram-bots/rows";
import { telegramText, type TelegramKey } from "@/core/telegram-bots/text";
import { displayNameOf, suggestUsername } from "@/core/telegram-bots/username";
import {
  type AgentTarget,
  serverTargetOf,
  telegramTargetOf,
} from "@/core/telegram-bots/web-pages";
import { useIsMobile } from "@/hooks/use-mobile";

import { openSettingsDialog } from "../settings/settings-dialog-store";

import { TelegramQrCode } from "./qr-code";
import { timeZonesWith } from "./settings-block";
import {
  closeTelegramSetup,
  forgetTelegramReplaced,
  holdTelegramConnect,
  openTelegramSetup,
  requestTelegramSettingsFocus,
  telegramConnectEpoch,
  useTelegramPageState,
} from "./setup-dialog-store";
import { TelegramTokenField, useWebhookConsent } from "./token-dialog";
import {
  refusalOther,
  useTelegramFailure,
  useTelegramValues,
} from "./use-telegram-failure";

const BOT_FATHER = "https://t.me/BotFather";
const WAIT_POLL_MS = 2000;
const HINT_AFTER_MS = 60_000;
/** A name can be one long word, which a button would hold on one line past the dialog. */
const WRAPS = "h-auto max-w-full whitespace-normal [overflow-wrap:anywhere]";

function browserTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone ?? "";
}

function botOf(bots: Bot[] | undefined, target: AgentTarget): Bot | null {
  const { agent_kind, agent_ref } = serverTargetOf(target);
  return (
    bots?.find(
      (bot) => bot.agent_kind === agent_kind && bot.agent_ref === agent_ref,
    ) ?? null
  );
}

interface DialogRefusal {
  key: TelegramKey;
  reason: string | null;
  other: string;
  linkId: string | null;
}

function TheParagraph({ hasBot }: { hasBot: boolean }) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const { voice, voiceProvider } = useTelegram();
  const [open, setOpen] = useState(!hasBot);
  if (!open) {
    return (
      <p className="text-muted-foreground text-sm">
        {strings.setup.before.again}{" "}
        <Button
          type="button"
          variant="link"
          className="h-auto p-0"
          onClick={() => setOpen(true)}
        >
          {strings.setup.before.show}
        </Button>
      </p>
    );
  }
  return (
    <section className="space-y-2 rounded-lg border p-3 text-sm">
      <h3 className="font-medium">{strings.setup.before.title}</h3>
      <p>{strings.setup.before.body}</p>
      {voice === "available" && voiceProvider ? (
        <p>
          {strings.setup.before.voice({
            transcriber: strings.transcriber[voiceProvider],
          })}
        </p>
      ) : null}
    </section>
  );
}

function StepOne({ target, name }: { target: AgentTarget; name: string }) {
  const { t } = useI18n();
  const strings = t.telegramBots.setup.step1;
  const [username, setUsername] = useState(() => suggestUsername(target));
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(username);
      setCopied(true);
    } catch {
      // The username stands in the text below, to copy by hand.
    }
  };

  return (
    <section className="space-y-2">
      <h3 className="font-medium">{strings.title}</h3>
      <Button
        type="button"
        onClick={() => {
          void copy();
          window.open(BOT_FATHER, "_blank", "noopener,noreferrer");
        }}
      >
        {strings.open}
      </Button>
      {copied ? (
        <p className="text-muted-foreground text-xs">
          {strings.copied({ username })}
        </p>
      ) : null}
      <ol className="list-inside list-disc space-y-1 text-sm">
        <li>{strings.a}</li>
        <li>{strings.b({ botName: displayNameOf(name) })}</li>
        <li>
          {strings.c({ username: "" })}
          <code className="font-mono">{username}</code>{" "}
          <Button
            type="button"
            variant="link"
            className="h-auto p-0"
            onClick={() => void copy()}
          >
            {strings.copyAgain}
          </Button>{" "}
          <Button
            type="button"
            variant="link"
            className="h-auto p-0"
            onClick={() => {
              setCopied(false);
              setUsername(suggestUsername(target));
            }}
          >
            {strings.another}
          </Button>
        </li>
        <li>{strings.d}</li>
      </ol>
      <p className="text-muted-foreground text-xs">{strings.public}</p>
      <details className="text-sm">
        <summary className="cursor-pointer">{strings.helpTitle}</summary>
        <ul className="mt-1 list-inside list-disc space-y-1">
          <li>{strings.helpStart}</li>
          <li>{strings.helpTaken}</li>
          <li>{strings.helpTooMany}</li>
          <li>{strings.helpCopy}</li>
        </ul>
      </details>
    </section>
  );
}

function StepTwo({
  target,
  name,
  needsZone,
  isFirst,
  onAdded,
}: {
  target: AgentTarget;
  name: string;
  needsZone: boolean;
  isFirst: boolean;
  onAdded: (first: boolean) => void;
}) {
  const { t, locale } = useI18n();
  const strings = t.telegramBots;
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();
  const values = useTelegramValues(name);
  const [zone, setZone] = useState("");
  const [askZone, setAskZone] = useState(needsZone);
  const [checking, setChecking] = useState(false);
  const [refusal, setRefusal] = useState<DialogRefusal | null>(null);
  const consent = useWebhookConsent();

  const send = async (token: string) => {
    setChecking(true);
    setRefusal(null);
    const epoch = telegramConnectEpoch();
    try {
      const answer = await addTelegramBot({
        ...serverTargetOf(target),
        token,
        timezone: zone || browserTimeZone(),
        locale,
        ...(consent.covers(token) ? { take_over_webhook: true } : {}),
      });
      holdTelegramConnect(answer.bot.id, answer.connect, epoch);
      onAdded(isFirst);
      await refresh();
    } catch (error) {
      const reason = error instanceof TelegramRefusal ? error.reason : null;
      if (reason === "webhook-set") {
        consent.refuse(token);
      }
      if (reason === "timezone-required" || reason === "invalid-timezone") {
        setAskZone(true);
      }
      if (reason === "agent-linked") {
        await refresh();
      }
      if (reason !== null && TELEGRAM_SECTION_REFUSALS.has(reason)) {
        failure.handled(error);
      }
      setRefusal({
        key:
          error instanceof TelegramRefusal
            ? telegramRefusalKey(error)
            : "refusal.failed",
        reason,
        other:
          error instanceof TelegramRefusal ? refusalOther(strings, error) : "",
        linkId:
          error instanceof TelegramRefusal ? (error.link_id ?? null) : null,
      });
    } finally {
      setChecking(false);
    }
  };

  const { data } = useTelegramBots();
  const linked = data?.bots.find((bot) => bot.id === refusal?.linkId);

  return (
    <section className="space-y-2">
      <h3 className="font-medium">{strings.setup.step2.title}</h3>
      {askZone ? (
        <div className="space-y-1">
          <p className="text-sm">{strings.setup.timezone.ask}</p>
          <div className="text-sm font-medium" id="telegram-setup-zone">
            {strings.setup.timezone.label}
          </div>
          <Select value={zone} onValueChange={setZone}>
            <SelectTrigger aria-labelledby="telegram-setup-zone">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {timeZonesWith("")
                .filter(Boolean)
                .map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
            </SelectContent>
          </Select>
        </div>
      ) : null}
      <TelegramTokenField
        disabled={checking || (askZone && !zone)}
        onToken={(token) => void send(token)}
        onNoToken={() =>
          setRefusal({
            key: "refusal.format",
            reason: null,
            other: "",
            linkId: null,
          })
        }
      />
      {checking ? (
        <p className="text-muted-foreground text-sm">
          {strings.setup.step2.checking}
        </p>
      ) : null}
      {refusal ? (
        <div className="space-y-2" role="alert">
          <p className="text-destructive text-sm">
            {telegramText(strings, refusal.key, {
              ...values,
              other: refusal.other,
            })}
          </p>
          {refusal.reason === "webhook-set" ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                consent.agree();
                setRefusal({
                  ...refusal,
                  key: "token.retryPaste",
                  reason: null,
                });
              }}
            >
              {strings.action.useHere}
            </Button>
          ) : null}
          {refusal.reason === "telegram-unreachable" ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setRefusal(null)}
            >
              {strings.action.tryAgain}
            </Button>
          ) : null}
          {linked ? (
            <Button asChild variant="outline" size="sm">
              <a href={linked.url} target="_blank" rel="noopener noreferrer">
                {strings.action.open}
              </a>
            </Button>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function StepThree({
  bot,
  name,
  connect,
  expired,
}: {
  bot: Bot;
  name: string;
  connect: Connect | undefined;
  expired: boolean;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const isMobile = useIsMobile();
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();
  const values = useTelegramValues(name);
  const [hint, setHint] = useState(false);
  const [line, setLine] = useState<TelegramKey | null>(null);

  useEffect(() => {
    const timer = setTimeout(() => setHint(true), HINT_AFTER_MS);
    return () => clearTimeout(timer);
  }, []);

  const fresh = async (opened: Window | null, open: boolean) => {
    setLine(null);
    const epoch = telegramConnectEpoch();
    try {
      const next = await requestTelegramConnect(bot.id);
      if (!holdTelegramConnect(bot.id, next, epoch)) {
        closeConnectWindow(opened);
      } else if (open) {
        openConnectUrl(next.url, opened);
      }
    } catch (error) {
      closeConnectWindow(opened);
      const refusal = error instanceof TelegramRefusal ? error : null;
      if (refusal?.reason !== "already-connected") {
        setLine(refusal ? telegramRefusalKey(refusal) : "refusal.failed");
      }
      if (refusal?.reason && TELEGRAM_SECTION_REFUSALS.has(refusal.reason)) {
        failure.handled(refusal);
      }
    }
    await refresh();
  };

  return (
    <section className="space-y-2">
      {bot.profile_synced ? null : (
        <p className="text-muted-foreground text-sm">
          {strings.setup.step2.accepted({ username: bot.bot_username })}
        </p>
      )}
      <h3 className="font-medium">{strings.setup.step3.title}</h3>
      {expired ? (
        <>
          <p className="text-sm">{strings.setup.step3.expired}</p>
          <Button
            type="button"
            variant="outline"
            onClick={() => void fresh(null, false)}
          >
            {strings.action.newLink}
          </Button>
        </>
      ) : (
        <>
          {connect ? (
            <Button asChild className={WRAPS}>
              <a href={connect.url} target="_blank" rel="noopener noreferrer">
                {strings.action.openChat({ name })}
              </a>
            </Button>
          ) : (
            <Button
              type="button"
              className={WRAPS}
              onClick={() => void fresh(prepareConnectWindow(), true)}
            >
              {strings.action.openChat({ name })}
            </Button>
          )}
          {connect && !isMobile ? (
            <TelegramQrCode
              value={connect.url}
              label={strings.setup.step3.qr}
            />
          ) : null}
        </>
      )}
      <p className="text-muted-foreground text-sm">{strings.setup.step3.why}</p>
      {expired ? null : (
        <p className="text-sm">{strings.setup.step3.waiting}</p>
      )}
      {!expired && hint ? (
        <p className="text-muted-foreground text-sm">
          {strings.setup.step3.hint}
        </p>
      ) : null}
      {line ? (
        <p className="text-destructive text-sm" role="alert">
          {telegramText(strings, line, values)}
        </p>
      ) : null}
    </section>
  );
}

/** A bot that cannot hear Start: the row's sentences, and its buttons one page on. */
function SetupFault({ lines }: { lines: RowLine[] }) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  return (
    <section className="space-y-2">
      {lines.map((line) => (
        <p key={line.key} className="text-sm">
          {telegramText(strings, line.key, line.values)}
        </p>
      ))}
      <Button
        type="button"
        onClick={() => {
          closeTelegramSetup();
          openSettingsDialog("channels");
        }}
      >
        {strings.webPage.telegram}
      </Button>
    </section>
  );
}

function DoneScreen({
  bot,
  name,
  firstBot,
}: {
  bot: Bot;
  name: string;
  firstBot: boolean;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const feature = useTelegram();
  const { data } = useTelegramBots();
  const { agents } = useAgents();
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();

  const settings = data?.settings;
  const blocked =
    sectionNoticeOf(feature, true) !== null ||
    (data !== undefined && data.limits.used >= data.limits.per_person);
  const next = blocked
    ? undefined
    : targetRowsOf({
        chatName: strings.chatName,
        agentNames: agents.map((agent) => agent.name),
        bots: data?.bots ?? [],
      }).find((row) => row.bot === null);

  const language =
    settings?.locale === "zh-CN"
      ? zhCN.locale.localName
      : enUS.locale.localName;

  return (
    <section className="space-y-3">
      <h3 className="font-medium">{strings.setup.done.title({ name })}</h3>
      <p className="text-sm">
        {strings.setup.done.body({ telegramName: bot.connected_as ?? "" })}
      </p>
      <p className="text-muted-foreground text-sm">
        {strings.setup.done.notYou}{" "}
        <Button
          type="button"
          variant="link"
          className="h-auto p-0"
          onClick={() => {
            const epoch = telegramConnectEpoch();
            void disconnectTelegramBot(bot.id)
              .then((answer) => {
                forgetTelegramReplaced(bot.id);
                holdTelegramConnect(bot.id, answer.connect, epoch);
                return refresh();
              })
              .catch((error: unknown) => failure.show(error, name));
          }}
        >
          {strings.action.disconnect}
        </Button>
      </p>
      {firstBot && settings ? (
        <p className="text-sm">
          {strings.setup.done.locale({
            language,
            timezone: settings.timezone,
          })}{" "}
          <Button
            type="button"
            variant="link"
            className="h-auto p-0"
            onClick={() => {
              closeTelegramSetup();
              requestTelegramSettingsFocus();
              openSettingsDialog("channels");
            }}
          >
            {strings.setup.done.change}
          </Button>
        </p>
      ) : null}
      {bot.profile_synced ? null : (
        <p className="text-muted-foreground text-sm">
          {strings.setup.done.profileLater}
        </p>
      )}
      <div className="flex flex-wrap gap-2">
        {next ? (
          <Button
            type="button"
            variant="outline"
            className={WRAPS}
            onClick={() => openTelegramSetup(next.target)}
          >
            {strings.setup.done.next({ nextName: next.name })}
          </Button>
        ) : null}
        <Button type="button" onClick={closeTelegramSetup}>
          {strings.setup.done.close}
        </Button>
      </div>
    </section>
  );
}

export function TelegramSetupDialog({ target }: { target: AgentTarget }) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const isMobile = useIsMobile();
  const feature = useTelegram();
  const reader = useTelegramReader();
  const { connects } = useTelegramPageState();
  const [firstBot, setFirstBot] = useState(false);
  const [phoneCode, setPhoneCode] = useState(false);
  const { data, isPending } = useTelegramBots({
    refetchInterval: (query) =>
      setupStageOf(botOf(query.state.data?.bots, target), Date.now()) ===
      "waiting"
        ? WAIT_POLL_MS
        : false,
  });

  const name = target.kind === "chat" ? strings.chatName : target.name;
  const now = useTelegramClock();
  const bot = botOf(data?.bots, target);
  const stage = setupStageOf(bot, now);
  const view = bot ? rowOf({ name, bot, feature, reader, now }) : null;
  const held = bot ? connects.get(bot.id) : undefined;
  const connect = held && Date.parse(held.expires_at) > now ? held : undefined;
  const ownAddress = useMemo(
    () =>
      typeof window === "undefined"
        ? ""
        : `${window.location.origin}/workspace?settings=channels&telegram=${telegramTargetOf(target)}`,
    [target],
  );

  return (
    <Dialog open onOpenChange={(open) => !open && closeTelegramSetup()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{strings.setup.title({ name })}</DialogTitle>
          <DialogDescription>{strings.setup.intro({ name })}</DialogDescription>
        </DialogHeader>
        {isPending ? (
          <p className="text-muted-foreground text-sm">{t.common.loading}</p>
        ) : stage === "done" && bot ? (
          <DoneScreen bot={bot} name={name} firstBot={firstBot} />
        ) : stage !== "steps" && view && stopsSetup(view.row) ? (
          <SetupFault lines={view.lines} />
        ) : stage !== "steps" && bot ? (
          <StepThree
            key={stage}
            bot={bot}
            name={name}
            connect={connect}
            expired={stage === "expired"}
          />
        ) : (
          <div className="space-y-4">
            {isMobile ? null : (
              <div className="space-y-2">
                <p className="text-muted-foreground text-sm">
                  {strings.setup.onPhone}{" "}
                  <Button
                    type="button"
                    variant="link"
                    className="h-auto p-0"
                    onClick={() => setPhoneCode(true)}
                  >
                    {strings.setup.onPhoneLink}
                  </Button>
                </p>
                {phoneCode ? (
                  <TelegramQrCode
                    value={ownAddress}
                    label={strings.setup.onPhoneQr}
                  />
                ) : null}
              </div>
            )}
            <TheParagraph hasBot={(data?.bots.length ?? 0) > 0} />
            <StepOne target={target} name={name} />
            <StepTwo
              target={target}
              name={name}
              needsZone={data?.settings === null && !browserTimeZone()}
              isFirst={data?.settings === null}
              onAdded={setFirstBot}
            />
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
