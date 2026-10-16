"use client";
import { useEffect, useState } from "react";

import type { TurnProgress } from "./types";

export function useWorkspaceStartingLong(progress: TurnProgress | null) {
  const run = progress?.runId;
  const stage = progress?.stage;
  const received = progress?.receivedAt;
  const identity = `${run}:${stage}:${received}`;
  const [long, setLong] = useState<string | null>(null);
  useEffect(() => {
    if (stage !== "workspace_starting") return;
    const remaining = Math.max(
      0,
      30_000 - (Date.now() - (received ?? Date.now())),
    );
    const timer = setTimeout(() => setLong(identity), remaining);
    return () => clearTimeout(timer);
  }, [identity, stage, received]);
  return stage === "workspace_starting" && long === identity;
}
