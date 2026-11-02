"use client";

import { useEffect, useId, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useI18n } from "@/core/i18n/hooks";
import {
  type Bot,
  newTelegramOperationId,
  replaceTelegramToken,
  TelegramRefusal,
  telegramRefusalKey,
} from "@/core/telegram-bots/api";
import { useRefreshTelegramBots } from "@/core/telegram-bots/hooks";
import { fingerprintOf } from "@/core/telegram-bots/picture";
import { type TelegramKey, telegramText } from "@/core/telegram-bots/text";
import { findToken } from "@/core/telegram-bots/token";

import {
  holdTelegramConnect,
  markTelegramReplaced,
  telegramConnectEpoch,
} from "./setup-dialog-store";
import {
  refusalOther,
  useTelegramFailure,
  useTelegramValues,
} from "./use-telegram-failure";

/**
 * The field of both token dialogs. The text is searched on a paste, on a press
 * and on Enter, and never while typing: the pattern has no upper bound, so a
 * token typed by hand would match before it is complete. Never on leaving the
 * field either, because that is how a person changes their mind. The field is
 * emptied as soon as a token is sent, so it is not kept in the page.
 */
export function TelegramTokenField({
  disabled,
  onToken,
  onNoToken,
}: {
  disabled: boolean;
  onToken: (token: string) => void;
  onNoToken: () => void;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const id = useId();
  const hintId = useId();
  const field = useRef<HTMLInputElement>(null);
  const mounted = useRef(true);
  const [text, setText] = useState("");

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const search = (value: string) => {
    const token = findToken(value);
    if (token) {
      setText("");
      onToken(token);
    } else {
      setText(value);
      onNoToken();
    }
  };

  const pasteFromClipboard = async () => {
    let value: string;
    try {
      value = await navigator.clipboard.readText();
    } catch {
      field.current?.focus();
      return;
    }
    if (mounted.current) {
      search(value);
    }
  };

  return (
    <form
      className="flex flex-col gap-2"
      onSubmit={(event) => {
        event.preventDefault();
        if (text) {
          search(text);
        }
      }}
    >
      <label className="text-sm font-medium" htmlFor={id}>
        {strings.setup.step2.label}
      </label>
      <div className="flex gap-2">
        <Input
          ref={field}
          id={id}
          type="password"
          autoComplete="new-password"
          aria-describedby={hintId}
          spellCheck={false}
          placeholder={strings.setup.step2.placeholder}
          value={text}
          disabled={disabled}
          onChange={(event) => setText(event.target.value)}
          onPaste={(event) => {
            event.preventDefault();
            search(event.clipboardData.getData("text"));
          }}
        />
        {/* A phone's keyboard can put text in without a paste, and has no Enter to speak of. */}
        {text ? (
          <Button type="submit" variant="outline" disabled={disabled}>
            {strings.setup.step2.check}
          </Button>
        ) : (
          <Button
            type="button"
            variant="outline"
            disabled={disabled}
            onClick={() => void pasteFromClipboard()}
          >
            {strings.setup.step2.paste}
          </Button>
        )}
      </div>
      <p id={hintId} className="text-muted-foreground text-xs">
        {strings.setup.step2.hint}
      </p>
    </form>
  );
}

/** Consent to take a webhook over covers the refused token and no other. */
export function useWebhookConsent() {
  const [refused, setRefused] = useState<string | null>(null);
  const [agreed, setAgreed] = useState<string | null>(null);
  return {
    refuse: (token: string) => setRefused(fingerprintOf(token)),
    agree: () => setAgreed(refused),
    agreed: agreed !== null,
    covers: (token: string) => agreed === fingerprintOf(token),
  };
}

interface Attempt {
  fingerprint: string;
  operationId: string;
}

/** A logical replacement keeps one id, so a retry returns its saved receipt. */
export function TelegramTokenDialog({
  bot,
  name,
  onClose,
}: {
  bot: Bot;
  name: string;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();
  const values = useTelegramValues(name);
  const attempt = useRef<Attempt | null>(null);
  const [checking, setChecking] = useState(false);
  const [lines, setLines] = useState<TelegramKey[]>([]);
  const consent = useWebhookConsent();
  const [other, setOther] = useState("");

  const finish = (key: TelegramKey) => {
    toast.success(telegramText(strings, key, values));
    void refresh();
    onClose();
  };

  const refused = (error: TelegramRefusal, token: string) => {
    switch (error.reason) {
      case "token-same":
        setLines(["token.same"]);
        return;
      case "token-invalid":
        setLines(["refusal.rejected"]);
        return;
      case "webhook-set":
        consent.refuse(token);
        setLines(["refusal.webhook"]);
        return;
      case "replacement-gone":
        finish("token.completedRemoved");
        return;
      default:
        if (failure.handled(error)) {
          onClose();
          return;
        }
    }
    setOther(refusalOther(strings, error));
    // Nothing was replaced, so the old bot stays and the list stays as it is.
    setLines([telegramRefusalKey(error), "token.replaceFailed"]);
    void refresh();
  };

  const send = async (token: string) => {
    const fingerprint = fingerprintOf(token);
    if (attempt.current?.fingerprint !== fingerprint) {
      attempt.current = { fingerprint, operationId: newTelegramOperationId() };
    }
    setChecking(true);
    setLines([]);
    const epoch = telegramConnectEpoch();
    try {
      const answer = await replaceTelegramToken(bot.id, {
        token,
        operation_id: attempt.current.operationId,
        ...(consent.covers(token) ? { take_over_webhook: true } : {}),
      });
      if (answer.replacement_status === "superseded") {
        finish("token.completedChanged");
        return;
      }
      // A successor, even when a replayed receipt carries no new address.
      const successor =
        answer.bot.id !== bot.id || answer.bot.bot_id !== bot.bot_id;
      if (successor) {
        markTelegramReplaced(answer.bot.id);
        if (answer.connect) {
          holdTelegramConnect(answer.bot.id, answer.connect, epoch);
        }
      }
      finish(
        answer.bot.health === "stopped"
          ? "token.savedRetry"
          : successor
            ? "toast.botReplaced"
            : "toast.tokenReplaced",
      );
    } catch (error) {
      // No answer says nothing about the outcome, so the same paste is repeated.
      if (error instanceof TelegramRefusal && error.reason !== null) {
        refused(error, token);
      } else {
        setLines(["token.retryPaste"]);
      }
    } finally {
      setChecking(false);
    }
  };

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{strings.token.title({ name })}</DialogTitle>
          <DialogDescription>{strings.token.body}</DialogDescription>
        </DialogHeader>
        <TelegramTokenField
          disabled={checking}
          onToken={(token) => void send(token)}
          onNoToken={() => setLines(["refusal.format"])}
        />
        {checking ? (
          <p className="text-muted-foreground text-sm">
            {strings.setup.step2.checking}
          </p>
        ) : null}
        {lines.map((key) => (
          <p key={key} className="text-destructive text-sm" role="alert">
            {telegramText(strings, key, { ...values, other })}
          </p>
        ))}
        {lines[0] === "refusal.webhook" ? (
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              consent.agree();
              setLines([]);
            }}
          >
            {strings.action.useHere}
          </Button>
        ) : null}
        {consent.agreed && !checking && lines.length === 0 ? (
          <p className="text-muted-foreground text-sm" role="alert">
            {strings.token.pasteOnceMore}
          </p>
        ) : null}
        <DialogFooter>
          <Button type="button" variant="ghost" onClick={onClose}>
            {t.common.cancel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
