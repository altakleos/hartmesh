"use client";

import { useId, useState } from "react";

import { Button } from "@/components/ui/button";
import { DialogFooter } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useI18n } from "@/core/i18n/hooks";
import {
  connectionDraft,
  formErrors,
  toServer,
  type ConnectionDraft,
  type FormErrors,
} from "@/core/mcp/connection-form";
import type { MCPServerConfig } from "@/core/mcp/types";

export function ConnectionForm({
  name,
  original,
  servers,
  pending,
  onSave,
  onJson,
  onCancel,
}: {
  name?: string;
  original?: MCPServerConfig;
  servers: Record<string, MCPServerConfig>;
  pending: boolean;
  onSave: (name: string, server: MCPServerConfig) => void;
  onJson: () => void;
  onCancel: () => void;
}) {
  const { t } = useI18n();
  const copy = t.settings.tools.connection;
  const initial = connectionDraft(name, original);
  const [draft, setDraft] = useState(initial);
  const dirty = Object.keys(initial).some(
    (key) =>
      draft[key as keyof ConnectionDraft] !==
      initial[key as keyof ConnectionDraft],
  );
  const [errors, setErrors] = useState<FormErrors>({});
  const [invalidPaste, setInvalidPaste] = useState(false);
  const id = useId();
  return (
    <form
      className="space-y-4"
      noValidate
      onSubmit={(event) => {
        event.preventDefault();
        if (pending) return;
        const issues = formErrors(draft, servers, !!original);
        if (invalidPaste) issues.token = "tokenFormat";
        setErrors(issues);
        if (!Object.keys(issues).length)
          onSave(draft.name, toServer(draft, original));
      }}
    >
      {(["name", "url", "token", "description"] as const).map((field) => {
        const error = errors[field];
        return (
          <div className="space-y-1" key={field}>
            <label className="text-sm font-medium" htmlFor={id + field}>
              {copy[field]}
            </label>
            <Input
              id={id + field}
              value={draft[field]}
              type={
                field === "token"
                  ? "password"
                  : field === "url"
                    ? "url"
                    : "text"
              }
              autoComplete={field === "token" ? "new-password" : "off"}
              disabled={pending || (field === "name" && !!original)}
              aria-invalid={!!error}
              aria-describedby={
                [
                  ...(field === "token" ? [id + "token-help"] : []),
                  ...(error ? [id + field + "-error"] : []),
                ].join(" ") || undefined
              }
              onPaste={
                field === "token"
                  ? (event) => {
                      // Password inputs remove pasted line breaks before onChange.
                      if (/[\r\n]/.test(event.clipboardData.getData("text"))) {
                        event.preventDefault();
                        setInvalidPaste(true);
                        setErrors((prev) => ({
                          ...prev,
                          token: "tokenFormat",
                        }));
                      }
                    }
                  : undefined
              }
              onChange={(event) => {
                const value = event.target.value;
                if (field === "token") setInvalidPaste(false);
                setDraft((prev) => ({
                  ...prev,
                  [field]: value,
                  tokenChanged: prev.tokenChanged || field === "token",
                }));
              }}
            />
            {field === "token" && (
              <p
                id={id + "token-help"}
                className="text-muted-foreground text-sm"
              >
                {copy.tokenHelp}
              </p>
            )}
            {error && (
              <p
                role="alert"
                id={id + field + "-error"}
                className="text-destructive text-sm"
              >
                {copy[error]}
              </p>
            )}
          </div>
        );
      })}
      <details>
        <summary>{copy.more}</summary>
        <label className="text-sm font-medium" htmlFor={id + "transport"}>
          {copy.transport}
        </label>
        <select
          id={id + "transport"}
          value={draft.type}
          disabled={pending}
          onChange={(event) =>
            setDraft({
              ...draft,
              type: event.target.value as ConnectionDraft["type"],
            })
          }
        >
          <option value="http">{copy.http}</option>
          <option value="sse">{copy.sse}</option>
        </select>
      </details>
      <Button type="button" variant="link" disabled={pending} onClick={onJson}>
        {dirty ? copy.discardJson : copy.json}
      </Button>
      <DialogFooter>
        <Button
          type="button"
          variant="outline"
          disabled={pending}
          onClick={onCancel}
        >
          {t.common.cancel}
        </Button>
        <Button type="submit" disabled={pending}>
          {pending ? t.common.loading : copy.saveCheck}
        </Button>
      </DialogFooter>
    </form>
  );
}
