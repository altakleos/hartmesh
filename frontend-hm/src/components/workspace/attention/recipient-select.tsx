import { useEffect, useId } from "react";

import { Button } from "@/components/ui/button";
import { usePersonNames } from "@/components/workspace/person-name";
import type { InputRequest } from "@/core/attention/api";
import { useInputRecipients } from "@/core/attention/recipients";
import { useI18n } from "@/core/i18n/hooks";

export const NO_RECIPIENT = "__none";

export function InputRecipientSelect({
  instance,
  purpose,
  enabled,
  value,
  choose,
  defaultLabel,
  validity,
}: {
  instance: string;
  purpose: InputRequest["purpose"];
  enabled: boolean;
  value: string;
  choose: (value: string) => void;
  defaultLabel?: string;
  validity: (valid: boolean) => void;
}) {
  const { t } = useI18n();
  const id = useId();
  const recipients = useInputRecipients(instance, purpose, enabled);
  const names = usePersonNames(recipients.grants.map((row) => row.user_id));
  // Grants decide eligibility; directory lookup supplies presentation only.
  const members = recipients.grants;
  const valid =
    enabled &&
    ((defaultLabel !== undefined && value === "") ||
      (defaultLabel === undefined && value === NO_RECIPIENT) ||
      (recipients.known && members.some((row) => row.user_id === value)));
  useEffect(() => validity(valid), [valid, validity]);
  return (
    <div>
      <label htmlFor={id}>{t.attention.recipient}</label>
      <select
        id={id}
        value={value}
        disabled={!enabled}
        onChange={(event) => choose(event.target.value)}
      >
        {defaultLabel !== undefined ? (
          <option value="">{defaultLabel}</option>
        ) : (
          <option value="" disabled>
            —
          </option>
        )}
        {members.map((row) => (
          <option key={row.user_id} value={row.user_id}>
            {names.get(row.user_id) ?? "…"}
          </option>
        ))}
        {defaultLabel === undefined && (
          <option value={NO_RECIPIENT}>{t.attention.recipientNone}</option>
        )}
      </select>
      {recipients.error && (
        <>
          <p role="alert">{t.attention.loadError}</p>
          <Button
            type="button"
            variant="outline"
            onClick={() => void recipients.refresh()}
          >
            {t.attention.refresh}
          </Button>
        </>
      )}
    </div>
  );
}
