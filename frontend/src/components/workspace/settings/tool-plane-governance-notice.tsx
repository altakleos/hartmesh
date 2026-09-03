import { ShieldCheckIcon, TriangleAlertIcon } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { useI18n } from "@/core/i18n/hooks";
import type {
  ToolPlaneGovernance,
  ToolPlaneRevisionState,
} from "@/core/tool-plane";

interface ToolPlaneGovernanceNoticeProps {
  governance: ToolPlaneGovernance | undefined;
  error: Error | null;
  isLoading: boolean;
  serviceUnavailable: boolean;
}

export function ToolPlaneGovernanceNotice({
  governance,
  error,
  isLoading,
  serviceUnavailable,
}: ToolPlaneGovernanceNoticeProps) {
  const { t } = useI18n();
  const copy = t.settings.toolPlane;

  if (serviceUnavailable) {
    return null;
  }
  if (isLoading) {
    return (
      <Alert>
        <ShieldCheckIcon />
        <AlertTitle>{copy.checking}</AlertTitle>
        <AlertDescription>{copy.mutationsDisabled}</AlertDescription>
      </Alert>
    );
  }
  if (error || !governance) {
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>{copy.loadFailed}</AlertTitle>
        <AlertDescription>
          {copy.mutationsDisabled}
          {error?.message ? " " + error.message : ""}
        </AlertDescription>
      </Alert>
    );
  }

  const { status, revisions } = governance;
  const latest = revisions[0];
  const stateCopy = status.drift
    ? copy.drift
    : {
        bootstrap_required: copy.bootstrapRequired,
        governed: copy.managed,
        unmanaged: copy.unmanaged,
        recovery_required: copy.recoveryRequired,
        immutable: copy.immutable,
      }[status.governance_state];
  const latestState = latest
    ? revisionStateLabel(latest.state, copy.states)
    : null;

  return (
    <Alert variant={status.drift ? "destructive" : "default"}>
      {status.drift ? <TriangleAlertIcon /> : <ShieldCheckIcon />}
      <AlertTitle>{copy.title}</AlertTitle>
      <AlertDescription>
        <p>{stateCopy}</p>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant={status.drift ? "destructive" : "secondary"}>
            {status.governance_state}
          </Badge>
          {status.active_revision_digest ? (
            <span>
              {copy.activeRevision}:{" "}
              <code>{shortDigest(status.active_revision_digest)}</code>
            </span>
          ) : (
            <span>{copy.noActiveRevision}</span>
          )}
          {latest && latestState ? (
            <span>
              {copy.latestRevision}:{" "}
              <code>{shortDigest(latest.revision_digest)}</code>{" "}
              <Badge
                variant={
                  latest.state === "rejected" ? "destructive" : "outline"
                }
              >
                {latestState}
              </Badge>
            </span>
          ) : (
            <span>{copy.noRevision}</span>
          )}
        </div>
        <p>{copy.credentialNotice}</p>
      </AlertDescription>
    </Alert>
  );
}

function shortDigest(digest: string): string {
  return digest.slice(0, 12) + "…";
}

function revisionStateLabel(
  state: ToolPlaneRevisionState,
  labels: Record<ToolPlaneRevisionState, string>,
): string {
  return labels[state];
}
