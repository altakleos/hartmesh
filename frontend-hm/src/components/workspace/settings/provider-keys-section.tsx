"use client";

import { useCallback, useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { fetch } from "@/core/api/fetcher";
import { useI18n } from "@/core/i18n/hooks";

import { SettingsSection } from "./settings-section";

type Source = "product" | "environment" | "none";

type ProviderKey = {
  provider: string;
  variable: string;
  kind: "models" | "tools";
  source: Source;
  product_key: "absent" | "set" | "unreadable";
  changed_at: string | null;
  changed_by: string | null;
};

type Refusal = { code: string; message: string };

type Status = {
  available: boolean;
  refusal: Refusal | null;
  providers: ProviderKey[];
};

type KeyEvent = {
  event_id: string;
  variable: string;
  action: "added" | "replaced" | "removed";
  actor_email: string | null;
  actor_id: string;
  occurred_at: string;
};

// The catalog's own names are file names; these are the ones people know.
const PROVIDER_NAMES: Record<string, string> = {
  openai: "OpenAI",
  anthropic: "Anthropic",
  gemini: "Google Gemini",
  deepseek: "DeepSeek",
  moonshot: "Moonshot AI",
  volcengine: "Volcengine",
  zai: "Z.ai",
  mimo: "Xiaomi MiMo",
  stepfun: "StepFun",
  novita: "Novita AI",
  atlascloud: "Atlas Cloud",
  tavily: "Tavily",
  serper: "Serper",
  brave: "Brave Search",
  exa: "Exa",
  firecrawl: "Firecrawl",
  serply: "Serply",
  groundroute: "GroundRoute",
  "tencent-wsa": "Tencent Web Search",
  fastcrw: "fastCRW",
};

// With no usable wrapping key nothing can be stored, but a stored key can
// still be removed: that is the way back to the deployment's own key.
const REMOVE_ONLY_REFUSALS = new Set([
  "no_wrapping_key",
  "wrapping_key_invalid",
]);

function providerName(id: string): string {
  return PROVIDER_NAMES[id] ?? id;
}

function formatWhen(value: string | null): string {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

type Copy = ReturnType<typeof useI18n>["t"]["settings"]["providerKeys"];

// The Gateway's messages name the deployment's variables, which are for
// whoever hosts it; an administrator reads what the refusal means for them.
function refusalCopy(copy: Copy, refusal: Refusal): string {
  switch (refusal.code) {
    case "no_wrapping_key":
      return copy.refusalNoWrappingKey;
    case "wrapping_key_invalid":
      return copy.refusalWrappingKeyInvalid;
    case "operator_model_file":
      return copy.refusalOperatorModelFile;
    case "render_refused":
      return copy.renderRefused;
    default:
      return refusal.message;
  }
}

async function readRefusal(res: Response): Promise<Refusal> {
  const fallback = { code: "", message: `HTTP ${res.status}` };
  try {
    const data = (await res.json()) as {
      detail?: Partial<Refusal> | string;
    };
    if (typeof data.detail === "string") {
      return { code: "", message: data.detail };
    }
    return {
      code: data.detail?.code ?? "",
      message: data.detail?.message ?? fallback.message,
    };
  } catch {
    return fallback;
  }
}

/**
 * Provider keys an administrator manages (GET/PUT/DELETE /api/provider-keys).
 *
 * Shown only where the deployment manages keys in the product and the
 * Gateway answers the list; elsewhere it renders nothing. A key typed here
 * lives in this component's state until it is sent, and nothing the
 * Gateway returns ever carries one back.
 */
export function ProviderKeysSection() {
  const { t } = useI18n();
  const copy = t.settings.providerKeys;
  const [status, setStatus] = useState<Status | null>(null);
  const [events, setEvents] = useState<KeyEvent[]>([]);
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const load = useCallback(async () => {
    const res = await fetch("/api/provider-keys");
    if (!res.ok) {
      setStatus(null);
      return;
    }
    const body = (await res.json()) as Status;
    setStatus(body);
    if (body.available) {
      const history = await fetch("/api/provider-keys/events?limit=5");
      if (history.ok) {
        setEvents(((await history.json()) as { events: KeyEvent[] }).events);
      }
    }
  }, []);

  useEffect(() => {
    void load().catch(() => setStatus(null));
  }, [load]);

  if (!status?.available) {
    return null;
  }

  const canSet = status.refusal === null;
  // Without a usable wrapping key the saved keys are intact but closed, and
  // setting one again is not on offer: what brings them back is the host's
  // setting, and removing one discards it.
  const hostSettingMissing =
    status.refusal !== null && REMOVE_ONLY_REFUSALS.has(status.refusal.code);
  const canRemove =
    status.refusal === null || REMOVE_ONLY_REFUSALS.has(status.refusal.code);
  const nameOf = (variable: string) =>
    providerName(
      status.providers.find((provider) => provider.variable === variable)
        ?.provider ?? variable,
    );

  const save = async (provider: ProviderKey) => {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const res = await fetch(
        `/api/provider-keys/${encodeURIComponent(provider.provider)}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ key: draft }),
        },
      );
      if (!res.ok) {
        setError(refusalCopy(copy, await readRefusal(res)));
        return;
      }
      setDraft("");
      setEditing(null);
      setNotice(
        copy.saved.replace("{provider}", providerName(provider.provider)),
      );
      await load();
    } catch {
      setError(t.settings.account.networkError);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (provider: ProviderKey) => {
    if (
      !window.confirm(
        copy.removeConfirm.replace(
          "{provider}",
          providerName(provider.provider),
        ),
      )
    ) {
      return;
    }
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const res = await fetch(
        `/api/provider-keys/${encodeURIComponent(provider.provider)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        setError(refusalCopy(copy, await readRefusal(res)));
        return;
      }
      const body = (await res.json()) as { provider: ProviderKey };
      const template =
        body.provider.source === "environment"
          ? copy.removedToEnvironment
          : copy.removedToNone;
      setNotice(
        template.replace("{provider}", providerName(provider.provider)),
      );
      await load();
    } catch {
      setError(t.settings.account.networkError);
    } finally {
      setBusy(false);
    }
  };

  const sourceBadge = (provider: ProviderKey) => {
    if (provider.product_key === "unreadable") {
      return <Badge variant="destructive">{copy.unreadable}</Badge>;
    }
    if (provider.source === "product") {
      return <Badge>{copy.sourceProduct}</Badge>;
    }
    if (provider.source === "environment") {
      return <Badge variant="secondary">{copy.sourceEnvironment}</Badge>;
    }
    return <Badge variant="outline">{copy.sourceNone}</Badge>;
  };

  const row = (provider: ProviderKey) => {
    const name = providerName(provider.provider);
    const stored = provider.product_key !== "absent";
    return (
      <li
        key={provider.variable}
        className="flex flex-col gap-2 py-3"
        data-testid={`provider-key-${provider.provider}`}
      >
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="min-w-0 space-y-0.5">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium">{name}</span>
              {sourceBadge(provider)}
            </div>
            <div className="text-muted-foreground font-mono text-xs">
              {provider.variable}
            </div>
            {stored && provider.changed_at && (
              <div className="text-muted-foreground text-xs">
                {copy.changed
                  .replace("{date}", formatWhen(provider.changed_at))
                  .replace("{who}", provider.changed_by ?? "")}
              </div>
            )}
            {provider.product_key === "unreadable" && (
              <p className="text-muted-foreground text-xs">
                {hostSettingMissing
                  ? copy.unreadableHintHostSetting
                  : copy.unreadableHint}
              </p>
            )}
          </div>
          {editing !== provider.variable && (
            <div className="flex gap-2">
              {canSet && (
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    setEditing(provider.variable);
                    setDraft("");
                    setError("");
                    setNotice("");
                  }}
                >
                  {stored ? copy.replace : copy.set}
                </Button>
              )}
              {canRemove && stored && (
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  disabled={busy}
                  onClick={() => void remove(provider)}
                >
                  {copy.remove}
                </Button>
              )}
            </div>
          )}
        </div>
        {editing === provider.variable && (
          <form
            className="flex max-w-md flex-wrap items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              void save(provider);
            }}
          >
            <Input
              type="password"
              autoComplete="off"
              spellCheck={false}
              className="min-w-0 flex-1"
              aria-label={copy.keyLabel.replace("{provider}", name)}
              placeholder={copy.keyPlaceholder}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              required
            />
            <Button type="submit" size="sm" disabled={busy || !draft.trim()}>
              {busy ? copy.saving : copy.save}
            </Button>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              disabled={busy}
              onClick={() => {
                setEditing(null);
                setDraft("");
              }}
            >
              {copy.cancel}
            </Button>
          </form>
        )}
      </li>
    );
  };

  const group = (kind: ProviderKey["kind"], title: string) => (
    <div className="space-y-1">
      <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
        {title}
      </h3>
      <ul className="divide-border divide-y">
        {status.providers
          .filter((provider) => provider.kind === kind)
          .map((provider) => row(provider))}
      </ul>
    </div>
  );

  const eventLine = (event: KeyEvent) => {
    const template =
      event.action === "added"
        ? copy.eventAdded
        : event.action === "replaced"
          ? copy.eventReplaced
          : copy.eventRemoved;
    return template
      .replace("{who}", event.actor_email ?? event.actor_id)
      .replace("{provider}", nameOf(event.variable));
  };

  return (
    <SettingsSection title={copy.title} description={copy.description}>
      <div className="space-y-6">
        {status.refusal && (
          <p className="text-muted-foreground text-sm" role="note">
            {refusalCopy(copy, status.refusal)}
          </p>
        )}
        {notice && (
          <p className="text-sm" role="status">
            {notice}
          </p>
        )}
        {error && <p className="text-sm text-red-500">{error}</p>}
        {group("models", copy.groupModels)}
        {group("tools", copy.groupTools)}
        {events.length > 0 && (
          <div className="space-y-1">
            <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
              {copy.recentChanges}
            </h3>
            <ul className="space-y-1">
              {events.map((event) => (
                <li key={event.event_id} className="text-sm">
                  {eventLine(event)}
                  <span className="text-muted-foreground">
                    {" · "}
                    {formatWhen(event.occurred_at)}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </SettingsSection>
  );
}
