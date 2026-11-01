"use client";

import { MessageCircleIcon } from "lucide-react";

import { CHAT_COLOR, pictureOf } from "@/core/telegram-bots/picture";
import type { AgentTarget } from "@/core/telegram-bots/web-pages";

/** The picture the bot has in Telegram, so both look like the same thing. */
export function BotPicture({ target }: { target: AgentTarget }) {
  const { initials, color } =
    target.kind === "custom_agent"
      ? pictureOf(target.name, target.name)
      : { initials: "", color: CHAT_COLOR };
  return (
    <span
      aria-hidden="true"
      className="flex size-10 shrink-0 items-center justify-center rounded-full text-sm font-semibold text-white"
      style={{ backgroundColor: color }}
    >
      {initials || <MessageCircleIcon className="size-5" />}
    </span>
  );
}
