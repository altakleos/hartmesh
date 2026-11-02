"use client";

import dynamic from "next/dynamic";
import { useEffect } from "react";
import { createPortal } from "react-dom";

import { useI18n } from "@/core/i18n/hooks";
import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import {
  discardTelegramConnects,
  resetTelegramPageState,
  useTelegramPageState,
} from "./setup-dialog-store";

/** In the body, after Settings: most presses come from there, and it covers the page. */
function Loading() {
  const { t } = useI18n();
  return createPortal(
    <div className="bg-background/80 fixed inset-0 z-50 grid place-items-center backdrop-blur-sm">
      <p role="status" className="text-muted-foreground text-sm">
        {t.common.loading}
      </p>
    </div>,
    document.body,
  );
}

// Loaded on first use: most sessions never open it, and it carries the QR encoder.
// On a slow phone that can take seconds, and a press must not look ignored.
const TelegramSetupDialog = dynamic(
  () => import("./setup-dialog").then((module) => module.TelegramSetupDialog),
  { loading: () => <Loading /> },
);

/**
 * The one set-up dialog, opened from every place with `openTelegramSetup`. It sits
 * under the provider rebuilt per account, and the store does not, so what the
 * store holds ends here: a shared tab must not offer one person's agent to the next.
 */
export function TelegramSetupHost() {
  const { setup } = useTelegramPageState();
  useEffect(
    () => () => {
      discardTelegramConnects();
      resetTelegramPageState();
    },
    [],
  );
  return setup ? (
    <TelegramSetupDialog key={telegramTargetOf(setup)} target={setup} />
  ) : null;
}
