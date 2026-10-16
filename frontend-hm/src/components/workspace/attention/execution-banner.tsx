import { ExecutionNotice } from "@/components/workspace/instances/execution-notice";
import { useAuth } from "@/core/auth/AuthProvider";
import { useBranding, useStorage } from "@/core/features";
import type { AgentInstancesFeature } from "@/core/features/api";

export function AttentionExecutionBanner({
  feature,
}: {
  feature: AgentInstancesFeature;
}) {
  const { user } = useAuth();
  const branding = useBranding();
  const storage = useStorage();
  return (
    <ExecutionNotice
      area="agent"
      reason={feature.reason}
      administrator={user?.system_role === "admin"}
      provider={branding.providerName}
      supportURL={branding.supportURL}
      storageReason={storage?.reason}
    />
  );
}
