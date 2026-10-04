"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { z } from "zod";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { fetch } from "@/core/api/fetcher";
import { useFileActionLifetime } from "@/core/file-areas/file-action-lifetime";
import { useI18n } from "@/core/i18n/hooks";
import { MODELS_QUERY_KEY } from "@/core/models/hooks";

import { SettingsSection } from "./settings-section";

const providerKeySchema = z.object({
  provider: z.string(),
  variable: z.string(),
  kind: z.enum(["models", "tools"]),
  source: z.enum(["product", "environment", "none"]),
  product_key: z.enum(["absent", "set", "unreadable"]),
  changed_at: z.string().nullable(),
  changed_by: z.string().nullable(),
});
const refusalSchema = z.object({ code: z.string(), message: z.string() });
const statusSchema = z.object({
  available: z.boolean(),
  refusal: refusalSchema.nullable(),
  providers: z.array(providerKeySchema),
});
const historySchema = z.object({
  events: z.array(
    z.object({
      event_id: z.string(),
      variable: z.string(),
      action: z.enum(["added", "replaced", "removed"]),
      actor_email: z.string().nullable(),
      actor_id: z.string(),
      occurred_at: z.string(),
    }),
  ),
});
type ProviderKey = z.infer<typeof providerKeySchema>;
type Refusal = z.infer<typeof refusalSchema>;
type Status = z.infer<typeof statusSchema>;
type KeyEvent = z.infer<typeof historySchema>["events"][number];

const STATUS_TIMEOUT_MS = 10_000;

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
 * Explicitly unmanaged deployments render nothing. Other failed reads retain
 * known metadata with a recovery action. A key typed here
 * lives in this component's state until it is sent, and nothing the
 * Gateway returns ever carries one back.
 */
export function ProviderKeysSection() {
  const { t } = useI18n();
  const copy = t.settings.providerKeys;
  const queryClient = useQueryClient();
  const account = useFileActionLifetime();
  const mutationActive = useRef(false);
  const lifetime = useRef<AbortController | null>(null);
  const activeLoad = useRef<AbortController | null>(null);
  const loadGeneration = useRef(0);
  const probe = useRef<AbortController | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<
    "accepted" | "rejected" | "inconclusive" | "not_testable" | "error" | null
  >(null);
  const [testReason, setTestReason] = useState<"timeout" | "http_429" | null>(
    null,
  );
  const retireTest = () => {
    probe.current?.abort();
    probe.current = null;
    setTesting(false);
    setTestResult(null);
    setTestReason(null);
  };
  const [status, setStatus] = useState<Status | null>(null);
  const [loadState, setLoadState] = useState<"loading" | "ready" | "error">(
    "loading",
  );
  const [events, setEvents] = useState<KeyEvent[]>([]);
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const load = useCallback(
    async (signal: AbortSignal) => {
      const accountSignal = account.signal;
      if (signal.aborted || accountSignal.aborted || !account.active) return;
      activeLoad.current?.abort();
      const generation = ++loadGeneration.current;
      const controller = new AbortController();
      activeLoad.current = controller;
      const current = () =>
        !signal.aborted &&
        !accountSignal.aborted &&
        loadGeneration.current === generation;
      let confirmed = false;
      let interrupt!: () => void;
      const interrupted = new Promise<never>((_resolve, reject) => {
        interrupt = () => reject(new DOMException("Aborted", "AbortError"));
      });
      const abort = () => controller.abort();
      controller.signal.addEventListener("abort", interrupt, { once: true });
      signal.addEventListener("abort", abort, { once: true });
      accountSignal.addEventListener("abort", abort, { once: true });
      const timeout = setTimeout(abort, STATUS_TIMEOUT_MS);
      const read = async () => {
        const res = await fetch("/api/provider-keys", {
          signal: controller.signal,
        });
        if (!current()) return;
        controller.signal.throwIfAborted();
        if (!res.ok) throw new Error("Provider status unavailable");
        const body: unknown = await res.json();
        if (!current()) return;
        controller.signal.throwIfAborted();
        const parsed = statusSchema.safeParse(body);
        if (!parsed.success) throw new Error("Invalid provider status");
        confirmed = true;
        setStatus(parsed.data);
        setLoadState("ready");
        if (!parsed.data.available) return;
        // Optional history cannot turn a confirmed status or successful mutation
        // into a failure. The shared deadline also bounds its body read.
        try {
          const history = await fetch("/api/provider-keys/events?limit=5", {
            signal: controller.signal,
          });
          if (!current() || controller.signal.aborted || !history.ok) return;
          const nextBody: unknown = await history.json();
          if (!current() || controller.signal.aborted) return;
          const nextEvents = historySchema.safeParse(nextBody);
          if (nextEvents.success) setEvents(nextEvents.data.events);
        } catch {
          /* Preserve the last known history. */
        }
      };
      setLoadState("loading");
      try {
        // Aborting fetch alone cannot settle a transport/body that ignores its
        // signal. Race the entire read and retire its continuations in finally.
        await Promise.race([read(), interrupted]);
      } catch {
        if (current() && !confirmed) setLoadState("error");
      } finally {
        clearTimeout(timeout);
        signal.removeEventListener("abort", abort);
        accountSignal.removeEventListener("abort", abort);
        controller.signal.removeEventListener("abort", interrupt);
        controller.abort();
        if (activeLoad.current === controller) activeLoad.current = null;
      }
    },
    [account],
  );

  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    // Run after parent effects: StrictMode refreshes the account signal during
    // setup, while a retired component's queued read is already aborted.
    void Promise.resolve().then(() => load(controller.signal));
    return () => {
      controller.abort();
      probe.current?.abort();
      probe.current = null;
    };
  }, [load, queryClient]);

  if (status?.available === false) {
    return null;
  }

  const loadFeedback =
    loadState === "loading" ? (
      <p className="text-muted-foreground text-sm" role="status">
        {copy.loading}
      </p>
    ) : loadState === "error" ? (
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm" role="alert">
          {copy.loadError}
        </p>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => {
            const owner = lifetime.current;
            if (owner) void load(owner.signal);
          }}
        >
          {copy.retry}
        </Button>
      </div>
    ) : null;

  if (!status) {
    return (
      <SettingsSection title={copy.title} description={copy.description}>
        {loadFeedback}
      </SettingsSection>
    );
  }
  const statusPending = loadState !== "ready";

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

  const refreshModelsAndStatus = async (
    signal: AbortSignal,
    accountSignal: AbortSignal,
  ) => {
    if (!account.active || accountSignal.aborted) return;
    // The mutation already succeeded even if a later status refresh fails.
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: MODELS_QUERY_KEY }),
      signal.aborted ? Promise.resolve() : load(signal),
    ]);
  };

  const testKey = async (provider: ProviderKey) => {
    if (
      provider.kind !== "models" ||
      statusPending ||
      !canSet ||
      !account.active
    )
      return;
    if (probe.current) return;
    retireTest();
    const owner = lifetime.current;
    const accountSignal = account.signal;
    if (!owner || owner.signal.aborted || accountSignal.aborted) return;
    const controller = new AbortController();
    probe.current = controller;
    const abort = () => controller.abort();
    owner.signal.addEventListener("abort", abort, { once: true });
    accountSignal.addEventListener("abort", abort, { once: true });
    const current = () =>
      !owner.signal.aborted &&
      !accountSignal.aborted &&
      !controller.signal.aborted &&
      probe.current === controller;
    setTesting(true);
    try {
      const res = await fetch(
        `/api/provider-keys/${encodeURIComponent(provider.provider)}/test`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ key: draft }),
          signal: controller.signal,
        },
      );
      if (!current()) return;
      if (!res.ok) {
        setTestResult("error");
        return;
      }
      const body = (await res.json()) as { result?: unknown; reason?: unknown };
      if (!current()) return;
      // Only fixed outcomes become UI text. Provider/model/reason fields never
      // echo a credential or provider-controlled error into the page.
      const result = body.result;
      setTestReason(
        result === "inconclusive" &&
          (body.reason === "timeout" || body.reason === "http_429")
          ? body.reason
          : null,
      );
      setTestResult(
        result === "accepted" ||
          result === "rejected" ||
          result === "inconclusive" ||
          result === "not_testable"
          ? result
          : "error",
      );
    } catch {
      if (current()) setTestResult("error");
    } finally {
      owner.signal.removeEventListener("abort", abort);
      accountSignal.removeEventListener("abort", abort);
      if (current()) {
        probe.current = null;
        setTesting(false);
      }
    }
  };

  const save = async (provider: ProviderKey) => {
    const signal = lifetime.current?.signal;
    const accountSignal = account.signal;
    if (
      !signal ||
      signal.aborted ||
      accountSignal.aborted ||
      !account.active ||
      mutationActive.current ||
      statusPending ||
      !canSet
    )
      return;
    mutationActive.current = true;
    retireTest();
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
          signal: accountSignal,
        },
      );
      if (!account.active || accountSignal.aborted) return;
      if (!res.ok) {
        const refusal = await readRefusal(res);
        if (!signal.aborted && !accountSignal.aborted && account.active)
          setError(refusalCopy(copy, refusal));
        return;
      }
      if (!signal.aborted) {
        setDraft("");
        setEditing(null);
        setNotice(
          copy.saved.replace("{provider}", providerName(provider.provider)),
        );
      }
      await refreshModelsAndStatus(signal, accountSignal);
    } catch {
      if (!signal.aborted && !accountSignal.aborted && account.active)
        setError(t.settings.account.networkError);
    } finally {
      mutationActive.current = false;
      if (!signal.aborted && !accountSignal.aborted && account.active)
        setBusy(false);
    }
  };

  const remove = async (provider: ProviderKey) => {
    if (statusPending || !canRemove) return;
    if (
      !window.confirm(
        copy.removeConfirm.replace(
          "{provider}",
          providerName(provider.provider),
        ),
      )
    )
      return;
    const signal = lifetime.current?.signal;
    const accountSignal = account.signal;
    if (
      !signal ||
      signal.aborted ||
      accountSignal.aborted ||
      !account.active ||
      mutationActive.current
    )
      return;
    mutationActive.current = true;
    retireTest();
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const res = await fetch(
        `/api/provider-keys/${encodeURIComponent(provider.provider)}`,
        { method: "DELETE", signal: accountSignal },
      );
      if (!account.active || accountSignal.aborted) return;
      if (!res.ok) {
        const refusal = await readRefusal(res);
        if (!signal.aborted && !accountSignal.aborted && account.active)
          setError(refusalCopy(copy, refusal));
        return;
      }
      // Refresh on HTTP success, independent of response-body rendering.
      const [body] = await Promise.all([
        res.json() as Promise<{ provider: ProviderKey }>,
        refreshModelsAndStatus(signal, accountSignal),
      ]);
      if (!signal.aborted && !accountSignal.aborted && account.active) {
        const template =
          body.provider.source === "environment"
            ? copy.removedToEnvironment
            : copy.removedToNone;
        setNotice(
          template.replace("{provider}", providerName(provider.provider)),
        );
      }
    } catch {
      if (!signal.aborted && !accountSignal.aborted && account.active)
        setError(t.settings.account.networkError);
    } finally {
      mutationActive.current = false;
      if (!signal.aborted && !accountSignal.aborted && account.active)
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
            {provider.kind === "tools" && (
              <p className="text-muted-foreground text-xs">
                {copy.testNotTestable}
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
                  disabled={busy || statusPending}
                  onClick={() => {
                    retireTest();
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
                  disabled={busy || statusPending}
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
              disabled={busy || statusPending || !canSet}
              onChange={(e) => {
                retireTest();
                setDraft(e.target.value);
              }}
              required
            />
            {provider.kind === "models" && (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={
                  busy || statusPending || !canSet || testing || !draft.trim()
                }
                onClick={() => void testKey(provider)}
              >
                {testing ? copy.testing : copy.test}
              </Button>
            )}
            <Button
              type="submit"
              size="sm"
              disabled={busy || statusPending || !canSet || !draft.trim()}
            >
              {busy ? copy.saving : copy.save}
            </Button>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              disabled={busy}
              onClick={() => {
                retireTest();
                setEditing(null);
                setDraft("");
              }}
            >
              {copy.cancel}
            </Button>
            {provider.kind === "models" && (
              <p className="text-muted-foreground basis-full text-xs">
                {copy.testHint}
              </p>
            )}
            {testResult && (
              <p role="status" className="basis-full text-sm">
                {
                  {
                    accepted: copy.testAccepted,
                    rejected: copy.testRejected,
                    inconclusive:
                      testReason === "timeout"
                        ? copy.testTimeout
                        : testReason === "http_429"
                          ? copy.testRateLimited
                          : copy.testInconclusive,
                    not_testable: copy.testNotTestable,
                    error: copy.testError,
                  }[testResult]
                }
              </p>
            )}
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
        {loadFeedback}
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
