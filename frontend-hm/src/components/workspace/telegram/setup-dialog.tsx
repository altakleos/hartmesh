"use client";

import { useQueryClient } from "@tanstack/react-query";
import { usePathname, useRouter } from "next/navigation";
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
import { hasCeiling } from "@/core/agent-instances/presentation";
import { useAgents } from "@/core/agents/hooks";
import { useAuth } from "@/core/auth/AuthProvider";
import type { ExecutionReason } from "@/core/features/api";
import { useAgentInstances, useTelegram } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import { enUS } from "@/core/i18n/locales/en-US";
import { zhCN } from "@/core/i18n/locales/zh-CN";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";
import {
  addTelegramBot,
  type Bot,
  type Connect,
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
  useTelegramInstanceDetail,
  useTelegramInstanceDiscovery,
} from "@/core/telegram-bots/instance-hooks";
import {
  instanceConnectBlock,
  type InstanceConnectBlock,
} from "@/core/telegram-bots/instance-state";
import {
  liveConnectOf,
  rowOf,
  type RowView,
  type TargetRow,
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
  webPageHref,
} from "@/core/telegram-bots/web-pages";
import { useIsMobile } from "@/hooks/use-mobile";

import { openSettingsDialog } from "../settings/settings-dialog-store";

import { TelegramExecutionNotice } from "./bot-row";
import { TelegramConnectNotice } from "./instance-controls";
import { TelegramQrCode } from "./qr-code";
import { timeZonesWith } from "./settings-block";
import {
  closeTelegramSetup,
  holdTelegramConnect,
  openTelegramSetup,
  requestTelegramSettingsFocus,
  telegramConnectEpoch,
  useTelegramPageState,
} from "./setup-dialog-store";
import { TelegramTokenField, useWebhookConsent } from "./token-dialog";
import {
  refusalOther,
  useTelegramConnect,
  useTelegramDisconnect,
  useTelegramFailure,
  useTelegramValues,
} from "./use-telegram-failure";

const BOT_FATHER = "https://t.me/BotFather";
const WAIT_POLL_MS = 2000;
const HINT_AFTER_MS = 60_000;
/** A button holds its words on one line, and a name can be long enough to leave the dialog. */
const WRAPS = "h-auto max-w-full whitespace-normal";

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
  execution?: { reason: ExecutionReason | null };
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
  const capture = useSpaceActionSignal(telegramTargetOf(target));
  const queryClient = useQueryClient();

  const send = async (token: string) => {
    setChecking(true);
    setRefusal(null);
    const epoch = telegramConnectEpoch();
    const signal = capture();
    try {
      const answer = await addTelegramBot(
        {
          ...serverTargetOf(target),
          token,
          timezone: zone || browserTimeZone(),
          locale,
          ...(consent.covers(token) ? { take_over_webhook: true } : {}),
        },
        signal,
      );
      signal.throwIfAborted();
      holdTelegramConnect(answer.bot.id, answer.connect, epoch);
      onAdded(isFirst);
      await refresh();
    } catch (error) {
      if (signal.aborted) return;
      const reason = error instanceof TelegramRefusal ? error.reason : null;
      if (reason === "webhook-set") {
        consent.refuse(token);
      }
      if (reason === "timezone-required" || reason === "invalid-timezone") {
        setAskZone(true);
      }
      // The bot may exist even so, when only the answer was lost. The list moves
      // the dialog on, where pasting the token again would say to make another.
      await refresh();
      if (signal.aborted) return;
      if (
        target.kind === "agent" &&
        [
          "policy-off",
          "execution-unavailable",
          "agent-not-found",
          "ceiling",
        ].includes(reason ?? "")
      ) {
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: ["agent-instances"] }),
          queryClient.invalidateQueries({ queryKey: ["features"] }),
        ]);
        if (signal.aborted) return;
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
        ...(reason === "execution-unavailable" &&
        error instanceof TelegramRefusal
          ? { execution: { reason: error.execution_reason } }
          : {}),
      });
    } finally {
      if (!signal.aborted) setChecking(false);
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
      {consent.agreed && !checking && !refusal ? (
        <p className="text-muted-foreground text-sm">
          {strings.token.pasteOnceMore}
        </p>
      ) : null}
      {refusal ? (
        <div className="space-y-2" role="alert">
          {refusal.execution ? (
            <TelegramExecutionNotice
              reason={refusal.execution.reason}
              name={name}
            />
          ) : (
            <p className="text-destructive text-sm">
              {telegramText(strings, refusal.key, {
                ...values,
                other: refusal.other,
              })}
            </p>
          )}
          {refusal.reason === "webhook-set" ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                consent.agree();
                setRefusal(null);
              }}
            >
              {strings.action.useHere}
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
  const failure = useTelegramFailure();
  const values = useTelegramValues(name);
  const [hint, setHint] = useState(false);
  const [line, setLine] = useState<TelegramKey | null>(null);
  const fresh = useTelegramConnect(bot, (error) => {
    const refusal = error instanceof TelegramRefusal ? error : null;
    setLine(refusal ? telegramRefusalKey(refusal) : "refusal.failed");
    if (refusal?.reason && TELEGRAM_SECTION_REFUSALS.has(refusal.reason)) {
      failure.handled(refusal);
    }
  });

  useEffect(() => {
    const timer = setTimeout(() => setHint(true), HINT_AFTER_MS);
    return () => clearTimeout(timer);
  }, []);

  const request = (open: boolean) => {
    setLine(null);
    void fresh.request({ open });
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
            disabled={fresh.busy}
            onClick={() => request(false)}
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
              disabled={fresh.busy}
              onClick={() => request(true)}
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

/** A bot that cannot hear Start: what its row says, and the way to the row's buttons. */
function SetupFault({ view, target }: { view: RowView; target: AgentTarget }) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const router = useRouter();
  const pathname = usePathname();
  const feature = useTelegram();
  // The row's one word; the reason stands in the section's notice.
  const notice = view.row === 1 ? sectionNoticeOf(feature, true) : null;
  const lines = notice ? [{ ...view.lines[0]!, key: notice.key }] : view.lines;
  const href = webPageHref("telegram", target, pathname);
  return (
    <section className="space-y-2">
      <p className="text-sm">{strings.setup.fault}</p>
      {lines.map((line) => (
        <p key={line.key} className="text-sm">
          {telegramText(strings, line.key, line.values)}
        </p>
      ))}
      {view.execution ? (
        <TelegramExecutionNotice
          reason={view.execution.reason}
          name={target.kind === "chat" ? undefined : target.name}
        />
      ) : null}
      <Button
        type="button"
        onClick={() => {
          closeTelegramSetup();
          if (href) {
            router.push(href);
          }
        }}
      >
        {strings.webPage.telegram}
      </Button>
    </section>
  );
}

/** Read candidate details in row order; an unreadable candidate never becomes a Connect. */
function NextTarget({ rows }: { rows: TargetRow[] }) {
  const row = rows[0];
  const detail = useTelegramInstanceDetail(row?.instance);
  const feature = useAgentInstances();
  const telegram = useTelegram();
  const { user } = useAuth();
  const { data } = useTelegramBots();
  const { t } = useI18n();
  if (!row) return null;
  if (row.target.kind === "agent" && detail.isPending) return null;
  if (
    row.target.kind === "agent" &&
    (detail.isError ||
      !detail.data ||
      instanceConnectBlock({
        instance: detail.data,
        feature,
        telegram,
        bots: data,
        canRead: !!user && hasCeiling(user.permissions, "agents:read"),
      }))
  )
    return (
      <NextTarget
        key={telegramTargetOf(rows[1]?.target ?? { kind: "chat" })}
        rows={rows.slice(1)}
      />
    );
  const target =
    row.target.kind === "agent" && detail.data
      ? {
          kind: "agent" as const,
          id: detail.data.id,
          name: detail.data.name,
          permissions: detail.data.permissions,
        }
      : row.target;
  return (
    <Button
      type="button"
      variant="outline"
      className={WRAPS}
      onClick={() => openTelegramSetup(target)}
    >
      {t.telegramBots.setup.done.next({
        nextName: target.kind === "chat" ? row.name : target.name,
      })}
    </Button>
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
  const leaving = useTelegramDisconnect();
  const discovery = useTelegramInstanceDiscovery();
  const { user } = useAuth();

  const settings = data?.settings;
  const blocked =
    sectionNoticeOf(feature, true) !== null ||
    (data !== undefined && data.limits.used >= data.limits.per_person);
  const next =
    blocked || discovery.pending || discovery.error
      ? []
      : targetRowsOf({
          chatName: strings.chatName,
          agentNames: agents.map((agent) => agent.name),
          bots: data?.bots ?? [],
          instances: discovery.instances,
          ownerId: user?.id,
        }).filter((row) => row.bot === null);

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
          disabled={leaving.busy}
          onClick={() => void leaving.disconnect(bot, name)}
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
        <NextTarget
          key={next[0] ? telegramTargetOf(next[0].target) : "none"}
          rows={next}
        />
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
  const agentsFeature = useAgentInstances();
  const { user } = useAuth();
  const detail = useTelegramInstanceDetail(
    target.kind === "agent"
      ? { id: target.id, permissions: target.permissions }
      : undefined,
  );
  const reader = useTelegramReader();
  const { connects } = useTelegramPageState();
  const [firstBot, setFirstBot] = useState(false);
  const [phoneCode, setPhoneCode] = useState(false);
  const { data, isPending, isError } = useTelegramBots({
    refetchInterval: (query) => {
      const seen = botOf(query.state.data?.bots, target);
      const stage = setupStageOf(seen, Date.now());
      // An expired link waits for nothing, unless the fault it hides can clear.
      return stage === "waiting" ||
        (stage === "expired" && seen?.health !== "ok")
        ? WAIT_POLL_MS
        : false;
    },
  });

  const name =
    target.kind === "chat"
      ? strings.chatName
      : (detail.data?.name ?? target.name);
  const now = useTelegramClock();
  const bot = botOf(data?.bots, target);
  const stage = setupStageOf(bot, now);
  const execution =
    agentsFeature.execution === "unavailable"
      ? { execution: "unavailable" as const, reason: agentsFeature.reason }
      : detail.data?.execution;
  const view = bot
    ? rowOf({
        name,
        bot,
        feature,
        reader,
        now,
        instance: target.kind === "agent" ? detail.data : undefined,
        execution,
      })
    : null;
  const canRead = !!user && hasCeiling(user.permissions, "agents:read");
  const block: InstanceConnectBlock | null =
    target.kind !== "agent"
      ? null
      : !canRead
        ? { kind: "ceiling" }
        : detail.isError
          ? { kind: "unreadable" }
          : detail.isPending
            ? { kind: "loading" }
            : detail.data
              ? instanceConnectBlock({
                  instance: detail.data,
                  feature: agentsFeature,
                  telegram: feature,
                  bots: data,
                  canRead,
                })
              : { kind: "use" };
  const unread =
    target.kind === "agent" && (!canRead || detail.isPending || detail.isError);
  const blocking = unread || (!bot && block);
  const explanation =
    block && detail.data ? (
      <TelegramConnectNotice block={block} instance={detail.data} />
    ) : block?.kind === "loading" ? (
      t.common.loading
    ) : block?.kind === "unreadable" ? (
      strings.unavailable.loadError
    ) : block?.kind === "ceiling" ? (
      strings.refusal.ceiling
    ) : (
      strings.refusal.agentGone({ name })
    );
  const connect = liveConnectOf(
    bot ? connects.get(bot.id) : undefined,
    bot,
    now,
  );
  const ownAddress = useMemo(
    () =>
      typeof window === "undefined"
        ? ""
        : `${window.location.origin}/workspace?settings=channels&telegram=${telegramTargetOf(target)}`,
    [target],
  );

  return (
    <Dialog open onOpenChange={(open) => !open && closeTelegramSetup()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto [overflow-wrap:anywhere]">
        <DialogHeader>
          <DialogTitle>{strings.setup.title({ name })}</DialogTitle>
          <DialogDescription>{strings.setup.intro({ name })}</DialogDescription>
        </DialogHeader>
        {/* The stages change by themselves, above all to Done, with the person away in
            Telegram. One region stays through all of them, as a live region must. */}
        <div role="status">
          {isPending ? (
            <p className="text-muted-foreground text-sm">{t.common.loading}</p>
          ) : isError && !data ? (
            <p>{strings.unavailable.loadError}</p>
          ) : blocking ? (
            <div className="text-sm">{explanation}</div>
          ) : bot && view && stopsSetup(view.row) ? (
            <SetupFault view={view} target={target} />
          ) : stage !== "steps" && bot && view ? (
            stage === "done" ? (
              <>
                {view.execution ? (
                  <TelegramExecutionNotice
                    reason={view.execution.reason}
                    name={name}
                    custody={detail.data?.custody}
                  />
                ) : null}
                <DoneScreen bot={bot} name={name} firstBot={firstBot} />
              </>
            ) : (
              <StepThree
                key={stage}
                bot={bot}
                name={name}
                connect={connect}
                expired={stage === "expired"}
              />
            )
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
              {target.kind === "agent" &&
              data?.read_by_others.includes(target.id) ? (
                <p className="text-sm">
                  {strings.setup.before.company({ name })}
                </p>
              ) : null}
              {target.kind === "agent" &&
              data?.policies.some(
                (policy) =>
                  policy.instance_id === target.id &&
                  policy.policy === "protected",
              ) ? (
                <p className="text-sm">{strings.setup.protected({ name })}</p>
              ) : null}
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
        </div>
      </DialogContent>
    </Dialog>
  );
}
