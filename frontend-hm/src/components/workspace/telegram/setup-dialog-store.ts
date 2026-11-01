"use client";

import { useSyncExternalStore } from "react";

import type { Connect } from "@/core/telegram-bots/api";
import type { AgentTarget } from "@/core/telegram-bots/web-pages";

interface TelegramPageState {
  /** The target whose set-up dialog is open. */
  setup: AgentTarget | null;
  /** The connect addresses this page received; only a `Connect` holds a code. */
  connects: ReadonlyMap<string, Connect>;
  /** Links that are the answer to a pasted token of another bot. */
  replaced: ReadonlySet<string>;
  /** The settings block takes the focus when it next appears. */
  focusSettings: boolean;
}

const listeners = new Set<() => void>();
const EMPTY: TelegramPageState = {
  setup: null,
  connects: new Map(),
  replaced: new Set(),
  focusSettings: false,
};
let state = EMPTY;
let connectEpoch = 0;

function update(next: Partial<TelegramPageState>) {
  state = { ...state, ...next };
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function useTelegramPageState(): TelegramPageState {
  return useSyncExternalStore(
    subscribe,
    () => state,
    () => EMPTY,
  );
}

export function openTelegramSetup(target: AgentTarget) {
  update({ setup: target });
}

export function closeTelegramSetup() {
  update({ setup: null });
}

export function requestTelegramSettingsFocus() {
  update({ focusSettings: true });
}

export function settleTelegramSettingsFocus() {
  update({ focusSettings: false });
}

export function telegramConnectEpoch(): number {
  return connectEpoch;
}

/** False for an answer to a request made before Disconnect all: its address is dead. */
export function holdTelegramConnect(
  linkId: string,
  connect: Connect,
  epoch: number,
): boolean {
  if (epoch !== connectEpoch) {
    return false;
  }
  update({ connects: new Map(state.connects).set(linkId, connect) });
  return true;
}

export function markTelegramReplaced(linkId: string) {
  update({ replaced: new Set(state.replaced).add(linkId) });
}

/** The next connect of this link is not an answer to a pasted token. */
export function forgetTelegramReplaced(linkId: string) {
  const replaced = new Set(state.replaced);
  replaced.delete(linkId);
  update({ replaced });
}

/** After Disconnect all no address held here works, and none may be offered. */
export function discardTelegramConnects() {
  connectEpoch += 1;
  update({ connects: new Map(), replaced: new Set() });
}

/** For a test: a module's state outlives a rendered tree. */
export function resetTelegramPageState() {
  update(EMPTY);
}
