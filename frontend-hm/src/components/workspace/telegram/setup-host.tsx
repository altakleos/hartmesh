"use client";

import dynamic from "next/dynamic";

import { telegramTargetOf } from "@/core/telegram-bots/web-pages";

import { useTelegramPageState } from "./setup-dialog-store";

// Loaded on first use: most sessions never open it, and it carries the QR encoder.
const TelegramSetupDialog = dynamic(() =>
  import("./setup-dialog").then((module) => module.TelegramSetupDialog),
);

/** The one set-up dialog, opened from every place with `openTelegramSetup`. */
export function TelegramSetupHost() {
  const { setup } = useTelegramPageState();
  return setup ? (
    <TelegramSetupDialog key={telegramTargetOf(setup)} target={setup} />
  ) : null;
}
