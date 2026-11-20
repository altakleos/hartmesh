import type { Message } from "@langchain/langgraph-sdk";

import { usePersonName } from "@/components/workspace/person-name";
import { useI18n } from "@/core/i18n/hooks";
import { getMessageRunId } from "@/core/messages/run-duration";
import { useRunDetail } from "@/core/threads/hooks";

export function PackageOriginLabel({
  threadId,
  message,
}: {
  threadId: string;
  message: Message;
}) {
  const claim = packageClaim(message);
  return claim ? (
    <VerifiedPackageOrigin threadId={threadId} claim={claim} />
  ) : null;
}

function packageClaim(message: Message) {
  const value: unknown = message.additional_kwargs?.deerflow_package_origin;
  if (
    !message.id ||
    !value ||
    typeof value !== "object" ||
    !("package" in value) ||
    typeof value.package !== "string" ||
    !value.package.trim() ||
    !("user_id" in value) ||
    typeof value.user_id !== "string" ||
    !value.user_id.trim() ||
    !("run_id" in value) ||
    typeof value.run_id !== "string" ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
      value.run_id,
    )
  )
    return null;
  const owner = getMessageRunId(message);
  if (owner && owner !== value.run_id) return null;
  return {
    source: value.package,
    person: value.user_id,
    run: value.run_id,
    message: message.id,
  };
}

function VerifiedPackageOrigin({
  threadId,
  claim,
}: {
  threadId: string;
  claim: NonNullable<ReturnType<typeof packageClaim>>;
}) {
  const { data: run, isError } = useRunDetail(threadId, claim.run, {
    immutableMetadataOnly: true,
  });
  const origin: unknown = run?.metadata?.deerflow_origin;
  const turn: unknown = run?.metadata?.deerflow_package_turn;
  // Older messages can contain arbitrary additional_kwargs. Only a matching
  // host-issued turn receipt establishes the author, including on live rows.
  if (
    isError ||
    run?.thread_id !== threadId ||
    run.run_id !== claim.run ||
    run.metadata?.requester_id !== claim.person ||
    !origin ||
    typeof origin !== "object" ||
    !("kind" in origin) ||
    origin.kind !== "extension" ||
    !("namespace" in origin) ||
    origin.namespace !== claim.source ||
    !turn ||
    typeof turn !== "object" ||
    !("run_id" in turn) ||
    turn.run_id !== claim.run ||
    !("message_ids" in turn) ||
    !Array.isArray(turn.message_ids) ||
    !turn.message_ids.includes(claim.message)
  )
    return null;
  return <PackageOrigin source={claim.source} person={claim.person} />;
}

function PackageOrigin({ source, person }: { source: string; person: string }) {
  const { t } = useI18n();
  const resolved = usePersonName(person, true);
  const label =
    resolved && resolved !== t.people.former && resolved !== t.people.unknown
      ? resolved
      : person;
  return (
    <p className="text-muted-foreground text-xs">
      {t.agentInstances.connections.packageOrigin(source, label)}
    </p>
  );
}
