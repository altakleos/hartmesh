export type EvidenceState =
  | "available"
  | "not_applicable"
  | "unsupported"
  | "legacy"
  | "pruned"
  | "unqualified"
  | "error";

export type QualificationState =
  | "qualified"
  | "unqualified"
  | "unverified"
  | "legacy"
  | "unsupported";

export interface EvidenceSection {
  state: EvidenceState;
  data: Record<string, unknown>;
}

export interface EvidenceTimelineItem {
  seq: number;
  at: string | null;
  kind: "policy_decision";
  decision: "warn" | "stop";
  reason_code: string;
  current: number;
  limit: number;
  state_digest: string;
}

export interface RunEvidenceSummaryV1 {
  schema: "hartmesh.run-evidence-summary";
  schema_version: 1;
  overview: {
    run_ref: string;
    thread_ref: string;
    status: string;
    accepted_at: string;
    updated_at: string;
    terminal_reason: string | null;
    policy: { profile: string; digest: string } | null;
    completeness: "complete" | "partial" | "in_progress";
  };
  timeline: EvidenceTimelineItem[];
  sections: Record<string, EvidenceSection>;
  qualification: { state: QualificationState };
}

export interface ThreadEvidence {
  runId: string;
  summary: RunEvidenceSummaryV1;
}

const evidenceStates = new Set<EvidenceState>([
  "available",
  "not_applicable",
  "unsupported",
  "legacy",
  "pruned",
  "unqualified",
  "error",
]);
const qualificationStates = new Set<QualificationState>([
  "qualified",
  "unqualified",
  "unverified",
  "legacy",
  "unsupported",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isBoundedString(value: unknown, maximum: number): value is string {
  return typeof value === "string" && value.length <= maximum;
}

function isTimelineItem(value: unknown): value is EvidenceTimelineItem {
  return (
    isRecord(value) &&
    Number.isSafeInteger(value.seq) &&
    (value.seq as number) >= 0 &&
    (value.at === null || isBoundedString(value.at, 64)) &&
    value.kind === "policy_decision" &&
    (value.decision === "warn" || value.decision === "stop") &&
    isBoundedString(value.reason_code, 64) &&
    Number.isSafeInteger(value.current) &&
    (value.current as number) >= 0 &&
    Number.isSafeInteger(value.limit) &&
    (value.limit as number) >= 0 &&
    typeof value.state_digest === "string" &&
    /^[0-9a-f]{64}$/.test(value.state_digest)
  );
}

export function parseEvidenceSummary(value: unknown): RunEvidenceSummaryV1 {
  if (
    !isRecord(value) ||
    value.schema !== "hartmesh.run-evidence-summary" ||
    value.schema_version !== 1
  ) {
    throw new Error("Unsupported evidence summary");
  }
  const overview = value.overview;
  const sections = value.sections;
  const qualification = value.qualification;
  if (
    !isRecord(overview) ||
    !isBoundedString(overview.run_ref, 128) ||
    !isBoundedString(overview.thread_ref, 128) ||
    !isBoundedString(overview.status, 32) ||
    !isBoundedString(overview.accepted_at, 64) ||
    !isBoundedString(overview.updated_at, 64) ||
    !(
      overview.terminal_reason === null ||
      isBoundedString(overview.terminal_reason, 64)
    ) ||
    !(
      overview.policy === null ||
      (isRecord(overview.policy) &&
        isBoundedString(overview.policy.profile, 64) &&
        typeof overview.policy.digest === "string" &&
        /^[0-9a-f]{64}$/.test(overview.policy.digest))
    ) ||
    !["complete", "partial", "in_progress"].includes(
      String(overview.completeness),
    ) ||
    !isRecord(sections) ||
    Object.keys(sections).length > 32 ||
    !isRecord(qualification) ||
    !qualificationStates.has(qualification.state as QualificationState) ||
    !Array.isArray(value.timeline) ||
    value.timeline.length > 100 ||
    !value.timeline.every(isTimelineItem)
  ) {
    throw new Error("Invalid evidence summary");
  }
  for (const section of Object.values(sections)) {
    if (
      !isRecord(section) ||
      !evidenceStates.has(section.state as EvidenceState) ||
      !isRecord(section.data)
    ) {
      throw new Error("Invalid evidence summary");
    }
  }
  return value as unknown as RunEvidenceSummaryV1;
}
