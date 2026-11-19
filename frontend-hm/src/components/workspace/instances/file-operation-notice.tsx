"use client";

import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  checkFileOperation,
  type CheckState,
} from "@/core/agent-instances/file-status";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

export function FileOperationNotice({
  state,
  file,
  conversation,
  check,
}: {
  state: CheckState;
  file: string;
  conversation?: string;
  check: () => void;
}) {
  const { t } = useI18n();
  const copy = t.agentInstances.files;
  const messages: Record<CheckState, string> = {
    checking: copy.checkingChange(file),
    pending: copy.pendingChange,
    unknown: copy.missingChange,
    unavailable: copy.unavailableCheck,
    denied: copy.deniedCheck,
    complete: copy.completedChange,
    "nothing-changed": copy.actionFailed,
    unconfirmed: conversation
      ? copy.unconfirmedUpload(file, conversation)
      : copy.unconfirmedChange,
  };
  return (
    <>
      <p role="status">{messages[state]}</p>
      {["pending", "unknown", "unavailable", "checking"].includes(state) && (
        <Button type="button" variant="outline" onClick={check}>
          {copy.checkAgain}
        </Button>
      )}
    </>
  );
}

export function PendingFileChange({
  instance,
  operation,
  file,
  changed,
  dismiss,
  cleanFailure,
}: {
  instance: string;
  operation: string;
  file: string;
  changed: () => void;
  dismiss?: () => void;
  cleanFailure?: () => void;
}) {
  const { t } = useI18n();
  const [attempt, setAttempt] = useState(0);
  const [state, setState] = useState<CheckState>("checking");
  const capture = useSpaceActionSignal(operation);
  const after = useRef(changed);
  after.current = changed;
  const failed = useRef(cleanFailure);
  failed.current = cleanFailure;
  useEffect(() => {
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, capture()]);
    void checkFileOperation(
      instance,
      { id: operation },
      signal,
      attempt === 0,
      (status) => {
        setState(status);
        if (status === "complete") after.current();
        if (status === "nothing-changed") failed.current?.();
      },
    );
    return () => controller.abort();
  }, [instance, operation, capture, attempt]);
  return (
    <article className="space-y-2 rounded border p-3">
      <p>{file}</p>
      <FileOperationNotice
        state={state}
        file={file}
        check={() => setAttempt((value) => value + 1)}
      />
      {dismiss && (
        <Button type="button" variant="outline" onClick={dismiss}>
          {t.common.close}
        </Button>
      )}
    </article>
  );
}
