"use client";

import { CheckIcon, CopyIcon } from "lucide-react";
import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { fetch, getCsrfHeaders } from "@/core/api/fetcher";
import { fetchSetupStatus } from "@/core/auth/setup";
import { parseAuthError } from "@/core/auth/types";
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
  const [localPasswords, setLocalPasswords] = useState(false);
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
        if (!cancelled) setLocalPasswords(status.sign_on_only !== true);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  if (!localPasswords) {
    return null;
  }

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
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
        setError(parseAuthError(data).message);
        return;
      }
      const body = data as { email: string; one_time_password: string };
      setAdded({ email: body.email, oneTimePassword: body.one_time_password });
      setEmail("");
      setCopied(false);
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
    <SettingsSection
      title={t.settings.account.addPersonTitle}
      description={t.settings.account.addPersonDescription}
    >
      {added ? (
        <div className="max-w-sm space-y-3" role="status">
          <p className="text-sm">
            {t.settings.account.addPersonAdded.replace("{email}", added.email)}
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
          {error && <p className="text-sm text-red-500">{error}</p>}
          <Button type="submit" variant="outline" size="sm" disabled={loading}>
            {loading
              ? t.settings.account.addPersonAdding
              : t.settings.account.addPersonSubmit}
          </Button>
        </form>
      )}
    </SettingsSection>
  );
}
