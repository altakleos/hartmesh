"use client";

import { useState } from "react";
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
import { useI18n } from "@/core/i18n/hooks";
import {
  acknowledgeTelegramConflict,
  type Bot,
  disconnectAllTelegramBots,
  disconnectTelegramBot,
  removeTelegramBot,
  retryTelegramBot,
  TelegramRefusal,
} from "@/core/telegram-bots/api";
import { useRefreshTelegramBots } from "@/core/telegram-bots/hooks";

import {
  discardTelegramConnects,
  forgetTelegramReplaced,
  holdTelegramConnect,
  telegramConnectEpoch,
} from "./setup-dialog-store";
import { useTelegramFailure } from "./use-telegram-failure";

export type TelegramRowDialogState =
  | {
      kind: "token" | "otherAccount" | "remove" | "thatWasMe" | "useHere";
      bot: Bot;
      name: string;
    }
  | { kind: "lost"; count: number };

function Confirm({
  title,
  body,
  button,
  destructive = false,
  onConfirm,
  onClose,
  extra,
}: {
  title: string;
  body: string;
  button: string;
  destructive?: boolean;
  onConfirm: () => Promise<void>;
  onClose: () => void;
  extra?: React.ReactNode;
}) {
  const { t } = useI18n();
  const [busy, setBusy] = useState(false);
  const confirm = async () => {
    setBusy(true);
    try {
      await onConfirm();
    } finally {
      setBusy(false);
      onClose();
    }
  };
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{body}</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button type="button" variant="ghost" onClick={onClose}>
            {t.common.cancel}
          </Button>
          {extra}
          <Button
            type="button"
            variant={destructive ? "destructive" : "default"}
            disabled={busy}
            onClick={() => void confirm()}
          >
            {button}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

/** The confirmations of a row and of the block for a lost phone. */
export function TelegramRowDialog({
  dialog,
  onClose,
  onPasteToken,
}: {
  dialog: Exclude<TelegramRowDialogState, { kind: "token" }>;
  onClose: () => void;
  onPasteToken: (bot: Bot, name: string) => void;
}) {
  const { t } = useI18n();
  const strings = t.telegramBots;
  const refresh = useRefreshTelegramBots();
  const failure = useTelegramFailure();

  if (dialog.kind === "lost") {
    return (
      <Confirm
        title={strings.confirm.lost.title({ count: dialog.count })}
        body={strings.confirm.lost.body}
        button={strings.lost.button}
        onClose={onClose}
        onConfirm={async () => {
          // Before the request too: its answer can be lost, and the bindings
          // may be closed even so. Nothing is lost, as Open chat gets a new one.
          discardTelegramConnects();
          try {
            await disconnectAllTelegramBots();
            toast.success(strings.toast.lost);
          } catch (error) {
            failure.show(error, "");
          }
          discardTelegramConnects();
          await refresh();
        }}
      />
    );
  }

  const { bot, name } = dialog;
  const ignored = (error: unknown, reason: string) =>
    error instanceof TelegramRefusal && error.reason === reason;

  switch (dialog.kind) {
    case "otherAccount":
      return (
        <Confirm
          title={strings.confirm.otherAccount.title}
          body={strings.confirm.otherAccount.body({
            telegramName: bot.connected_as ?? "",
          })}
          button={strings.confirm.otherAccount.button}
          onClose={onClose}
          onConfirm={async () => {
            const epoch = telegramConnectEpoch();
            try {
              const answer = await disconnectTelegramBot(bot.id);
              forgetTelegramReplaced(bot.id);
              holdTelegramConnect(bot.id, answer.connect, epoch);
              toast.success(strings.toast.disconnected);
            } catch (error) {
              if (!ignored(error, "not-connected")) {
                failure.show(error, name);
              }
            }
            await refresh();
          }}
        />
      );
    case "remove":
      return (
        <Confirm
          title={strings.confirm.remove.title({ name })}
          body={strings.confirm.remove.body({ name })}
          button={strings.confirm.remove.button}
          destructive
          onClose={onClose}
          onConfirm={async () => {
            try {
              await removeTelegramBot(bot.id);
              toast.success(strings.toast.removed);
            } catch (error) {
              failure.show(error, name);
            }
            await refresh();
          }}
        />
      );
    case "thatWasMe":
      return (
        <Confirm
          title={strings.confirm.thatWasMe.title}
          body={strings.confirm.thatWasMe.body}
          button={strings.confirm.thatWasMe.button}
          onClose={onClose}
          extra={
            <Button
              type="button"
              variant="outline"
              onClick={() => onPasteToken(bot, name)}
            >
              {strings.action.pasteToken}
            </Button>
          }
          onConfirm={async () => {
            try {
              await acknowledgeTelegramConflict(bot.id);
              toast.success(strings.toast.thatWasMe);
            } catch (error) {
              // Another tab already did it; the list shows the row as it is.
              if (!ignored(error, "no-conflict")) {
                failure.show(error, name);
              }
            }
            await refresh();
          }}
        />
      );
    case "useHere":
      return (
        <Confirm
          title={strings.confirm.useHere.title}
          body={strings.confirm.useHere.body}
          button={strings.action.useHere}
          onClose={onClose}
          onConfirm={async () => {
            try {
              await retryTelegramBot(bot.id, { takeOverWebhook: true });
              toast.success(strings.toast.usedHere);
            } catch (error) {
              failure.show(error, name);
            }
            await refresh();
          }}
        />
      );
  }
}
