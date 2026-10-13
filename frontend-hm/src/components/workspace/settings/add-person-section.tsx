"use client";

import { useQueryClient } from "@tanstack/react-query";
import { CheckIcon, CopyIcon } from "lucide-react";
import { useEffect, useId, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ProviderSupportLink } from "@/components/workspace/instances/execution-notice";
import { fetch, getCsrfHeaders } from "@/core/api/fetcher";
import { fetchSetupStatus } from "@/core/auth/setup";
import { parseAuthError } from "@/core/auth/types";
import { useSeats } from "@/core/features";
import { useStorageCopy } from "@/core/file-areas/storage-copy";
import { useI18n } from "@/core/i18n/hooks";

import { SettingsSection } from "./settings-section";

type Added = { email: string; oneTimePassword: string };

/**
 * An administrator adds a local-password account (POST /api/v1/auth/users).
 *
 * Offered only where local passwords are a way in: in sign-on-only mode the
 * identity provider decides who has an account, and the Gateway refuses the
 * route. The one-time password lives in this component's state and nowhere
 * else, so leaving the screen is the last time anyone sees it.
 */
export function AddPersonSection() {
  const { t } = useI18n();
  const seats = useSeats();
  const copy = useStorageCopy("files");
  const queryClient = useQueryClient();
  const noSeatsId = useId();
  const full = seats !== null && seats.inUse >= seats.allowed;
  const noSeatsText = t.settings.account.noSeats(
    seats?.allowed ?? null,
    copy.provider,
  );
  const [localPasswords, setLocalPasswords] = useState(false);
  const [setupKnown, setSetupKnown] = useState(false);
  const [email, setEmail] = useState("");
  const [added, setAdded] = useState<Added | null>(null);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    // Hidden until the Gateway answers: a form that then vanishes, or a
    // control that can only be refused, would both mislead.
    void fetchSetupStatus()
      .then((status) => {
        if (!cancelled) {
          setLocalPasswords(status.sign_on_only !== true);
          setSetupKnown(true);
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  if (!localPasswords) {
    return seats ? (
      <div className="space-y-2 text-sm">
        <p>{t.settings.account.seats(seats.inUse, seats.allowed)}</p>
        {setupKnown && full && (
          <>
            <p>
              {t.settings.account.noSeatsSignOn(seats.allowed, copy.provider)}
            </p>
            <ProviderSupportLink
              provider={copy.providerName}
              supportURL={copy.supportURL}
            />
          </>
        )}
      </div>
    ) : null;
  }

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (full) {
      setError(noSeatsText);
      return;
    }
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/v1/auth/users", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...getCsrfHeaders() },
        body: JSON.stringify({ email }),
      });
      const data: unknown = await res.json();
      if (!res.ok) {
        const refusal = parseAuthError(data);
        setError(
          refusal.code === "no_seats_left" ? noSeatsText : refusal.message,
        );
        if (refusal.code === "no_seats_left")
          void queryClient.invalidateQueries({ queryKey: ["features"] });
        return;
      }
      const body = data as { email: string; one_time_password: string };
      setAdded({ email: body.email, oneTimePassword: body.one_time_password });
      setEmail("");
      setCopied(false);
      void queryClient.invalidateQueries({ queryKey: ["features"] });
    } catch {
      setError(t.settings.account.networkError);
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async () => {
    if (!added) return;
    try {
      await navigator.clipboard.writeText(added.oneTimePassword);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  };

  return (
    <>
      {seats && (
        <p className="mb-2 text-sm">
          {t.settings.account.seats(seats.inUse, seats.allowed)}
        </p>
      )}
      <SettingsSection
        title={t.settings.account.addPersonTitle}
        description={t.settings.account.addPersonDescription}
      >
        {added ? (
          <div className="max-w-sm space-y-3" role="status">
            <p className="text-sm">
              {t.settings.account.addPersonAdded.replace(
                "{email}",
                added.email,
              )}
            </p>
            <div className="space-y-1">
              <span className="text-muted-foreground text-sm">
                {t.settings.account.addPersonOneTimePassword}
              </span>
              <div className="flex items-center gap-2">
                <code
                  className="bg-muted rounded px-2 py-1 font-mono text-sm break-all"
                  data-testid="one-time-password"
                >
                  {added.oneTimePassword}
                </code>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => void handleCopy()}
                  className="gap-1"
                >
                  {copied ? (
                    <CheckIcon className="size-4" />
                  ) : (
                    <CopyIcon className="size-4" />
                  )}
                  {copied
                    ? t.settings.account.addPersonCopied
                    : t.settings.account.addPersonCopy}
                </Button>
              </div>
            </div>
            <p className="text-muted-foreground text-sm">
              {t.settings.account.addPersonHandOver}
            </p>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setAdded(null)}
            >
              {t.settings.account.addPersonAnother}
            </Button>
          </div>
        ) : (
          <form onSubmit={handleAdd} className="max-w-sm space-y-3">
            <Input
              type="email"
              placeholder={t.settings.account.addPersonEmail}
              aria-label={t.settings.account.addPersonEmail}
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
            {error && !(full && error === noSeatsText) && (
              <p className="text-sm text-red-500" role="alert">
                {error}
              </p>
            )}
            <Button
              type="submit"
              variant="outline"
              size="sm"
              disabled={loading || full}
              aria-describedby={full ? noSeatsId : undefined}
            >
              {loading
                ? t.settings.account.addPersonAdding
                : t.settings.account.addPersonSubmit}
            </Button>
            {full && (
              <div id={noSeatsId} className="space-y-2 text-sm">
                <p>{noSeatsText}</p>
                <ProviderSupportLink
                  provider={copy.providerName}
                  supportURL={copy.supportURL}
                />
              </div>
            )}
          </form>
        )}
      </SettingsSection>
    </>
  );
}
