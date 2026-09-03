export type ToolPlaneScopeKind = "deployment_base" | "user_overlay";

export type ToolPlaneRevisionState =
  | "bootstrap_required"
  | "staged"
  | "validating"
  | "validated"
  | "rejected"
  | "prepared"
  | "promoted"
  | "superseded"
  | "recovery_required";

export type ToolPlaneGovernanceState =
  | "bootstrap_required"
  | "governed"
  | "unmanaged"
  | "recovery_required"
  | "immutable";

export interface ToolPlaneScope {
  version: 1;
  kind: ToolPlaneScopeKind;
  user_ref?: string;
}

export interface ToolPlaneStatus {
  version: 1;
  scope: ToolPlaneScope;
  governance_state: ToolPlaneGovernanceState;
  active_revision_id: string | null;
  active_revision_digest: string | null;
  generation: number;
  projection_digest: string | null;
  drift: boolean;
  immutable: boolean;
  durable: boolean;
  validation_policy_digest: string;
}

export interface ToolPlaneRevisionSummary {
  version: 1;
  revision_id: string;
  revision_digest: string;
  scope: ToolPlaneScope;
  content_digest: string;
  state: ToolPlaneRevisionState;
  staged_at: string;
  promoted_at: string | null;
}

export interface ToolPlaneRevisionList {
  version: 1;
  scope: ToolPlaneScope;
  revisions: ToolPlaneRevisionSummary[];
}

export interface ToolPlaneGovernance {
  status: ToolPlaneStatus;
  revisions: ToolPlaneRevisionSummary[];
}
