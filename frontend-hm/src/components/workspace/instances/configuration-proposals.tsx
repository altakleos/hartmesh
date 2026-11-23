"use client";

import {
  useInfiniteQuery,
  useQueryClient,
  type InfiniteData,
} from "@tanstack/react-query";
import { useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { PersonName } from "@/components/workspace/person-name";
import { InstanceApiError } from "@/core/agent-instances/api";
import {
  hasCeiling,
  INSPECT,
  MANAGE,
} from "@/core/agent-instances/presentation";
import {
  decideProposal,
  listProposals,
  type ConfigurationProposal,
  type ProposalPage,
} from "@/core/agent-instances/proposals";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { useSpaceActionSignal } from "@/core/spaces/lifetime";

import { useLifecycleActions } from "./lifecycle-actions";

export function ConfigurationProposals() {
  const { user } = useAuth();
  const { instance } = useLifecycleActions();
  if (
    !user ||
    !hasCeiling(user.permissions, "agents:read") ||
    (instance.permissions & (INSPECT | MANAGE)) !== (INSPECT | MANAGE)
  )
    return null;
  const scope = JSON.stringify([
    user.id,
    user.system_role,
    user.permissions,
    instance.id,
    instance.generation,
    instance.permissions,
  ]);
  return <ProposalList key={scope} scope={scope} />;
}

function ProposalList({ scope }: { scope: string }) {
  const {
    instance,
    canManage,
    refresh,
    pending,
    busy: lifecycleBusy,
  } = useLifecycleActions();
  const { t, locale } = useI18n();
  const zh = locale === "zh-CN";
  const title = useId();
  const client = useQueryClient();
  const key = ["employee-configuration-proposals", scope] as const;
  const signalForAction = useSpaceActionSignal(scope);
  const [sending, setSending] = useState(false);
  const submission = useRef(false);
  const [uncertain, setUncertain] = useState(false);
  const [error, setError] = useState<string>();
  const query = useInfiniteQuery({
    queryKey: key,
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) =>
      listProposals(instance.id, pageParam, signal),
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    retry: false,
    gcTime: 0,
    refetchOnWindowFocus: "always",
    refetchInterval: (query) =>
      query.state.data?.pages.some((page) =>
        page.proposals.some((row) => row.status === "applying"),
      )
        ? 3000
        : false,
  });
  const rows = query.data?.pages.flatMap((page) => page.proposals) ?? [];
  const refreshGeneration = useRef(instance.generation);
  useEffect(() => {
    const currentGeneration =
      query.data?.pages[0]?.proposals[0]?.current_generation;
    if (
      currentGeneration !== undefined &&
      currentGeneration > refreshGeneration.current
    ) {
      refreshGeneration.current = currentGeneration;
      void refresh();
    }
  }, [query.data, refresh]);

  async function decide(
    row: ConfigurationProposal,
    action: "adopt" | "decline",
  ) {
    if (submission.current || !canManage || uncertain || row.status !== "open")
      return;
    if (
      action === "adopt" &&
      (row.stale || row.adopt_blocked_reason || pending || lifecycleBusy)
    )
      return;
    submission.current = true;
    setSending(true);
    setError(undefined);
    const owner = signalForAction();
    try {
      const receipt = await decideProposal(
        instance.id,
        row.id,
        action,
        action === "adopt"
          ? {
              base_generation: row.base_generation,
              base_revision: row.base_revision,
              candidate_digest: row.candidate_digest,
            }
          : {},
        owner,
      );
      owner.throwIfAborted();
      if (
        receipt.id !== row.id ||
        receipt.instance_id !== instance.id ||
        receipt.candidate_digest !== row.candidate_digest ||
        (action === "decline"
          ? receipt.status !== "declined"
          : !["applying", "adopted", "abandoned"].includes(receipt.status))
      )
        throw new Error("Unexpected proposal receipt");
      await client.cancelQueries({ queryKey: key, exact: true });
      owner.throwIfAborted();
      client.setQueryData<InfiniteData<ProposalPage>>(
        key,
        (data) =>
          data && {
            ...data,
            pages: data.pages.map((page) => ({
              ...page,
              proposals: page.proposals.map((prev) =>
                prev.id === row.id ? receipt : prev,
              ),
            })),
          },
      );
      void refresh();
    } catch (failure) {
      if (owner.aborted) return;
      setUncertain(true);
      setError(
        failure instanceof InstanceApiError &&
          failure.reason === "work-in-flight"
          ? zh
            ? "员工正在工作。请在空闲时重新检查。"
            : "The employee is working. Check again when they are idle."
          : zh
            ? "请重新读取提议以确认当前状态。"
            : "Check the proposal again to confirm its current status.",
      );
    } finally {
      if (!owner.aborted) {
        submission.current = false;
        setSending(false);
      }
    }
  }
  async function checkStatus() {
    const owner = signalForAction();
    const result = await query.refetch();
    if (!owner.aborted && result.isSuccess) {
      setUncertain(false);
      setError(undefined);
    }
  }
  return (
    <section aria-labelledby={title} className="space-y-3">
      <h3 className="font-medium" id={title}>
        {zh ? "配置提议" : "Configuration proposals"}
      </h3>
      {query.isPending && <p role="status">{t.common.loading}</p>}
      {(query.isError || uncertain) && (
        <div role="alert">
          <p>
            {error ??
              (zh ? "暂时无法读取提议。" : "Proposals are unavailable.")}
          </p>
          <Button
            variant="outline"
            disabled={query.isFetching}
            onClick={() => void checkStatus()}
          >
            {t.agentInstances.refresh}
          </Button>
        </div>
      )}
      {query.isSuccess && !uncertain && (
        <>
          {rows.length === 0 && (
            <p>{zh ? "尚无配置提议。" : "No configuration proposals yet."}</p>
          )}
          {rows.map((row) => (
            <ProposalCard
              key={row.id}
              row={row}
              canDecide={canManage}
              busy={sending}
              adoptBusy={!!pending || lifecycleBusy}
              decide={(action) => void decide(row, action)}
            />
          ))}
          {query.hasNextPage && (
            <Button
              variant="outline"
              disabled={query.isFetching}
              onClick={() => void query.fetchNextPage()}
            >
              {t.agentInstances.more}
            </Button>
          )}
        </>
      )}
    </section>
  );
}

function ProposalCard({
  row,
  canDecide,
  busy,
  adoptBusy,
  decide,
}: {
  row: ConfigurationProposal;
  canDecide: boolean;
  busy: boolean;
  adoptBusy: boolean;
  decide: (action: "adopt" | "decline") => void;
}) {
  const { locale, t } = useI18n();
  const zh = locale === "zh-CN";
  const onOff = (on: boolean) =>
    on ? (zh ? "开启" : "On") : zh ? "关闭" : "Off";
  const status = {
    open: zh ? "待决定" : "Awaiting decision",
    applying: zh ? "应用中" : "Applying",
    adopted: zh ? "已采纳" : "Adopted",
    declined: zh ? "已拒绝" : "Declined",
    abandoned: zh ? "已放弃" : "Abandoned",
  }[row.status];
  const reasons = {
    stale: zh
      ? "员工配置已有变化。请让员工根据当前版本重新提议。"
      : "The employee’s configuration has changed. Ask for a fresh proposal against the current revision.",
    "different-definition-owner": zh
      ? "只有当前定义的所有者可以采纳此提议。"
      : "Only the current definition owner can adopt this proposal.",
    "definition-unavailable": zh
      ? "当前定义暂时无法访问。"
      : "The current definition is unavailable.",
    "home-admin-required": zh
      ? "采纳需要员工主目录的管理权限。"
      : "Adoption requires administrator access to the employee’s Home.",
    "employee-inactive": zh
      ? "只有活跃的员工才能应用此更改。"
      : "The employee must be active to apply this change.",
    "lifecycle-unavailable": zh
      ? "请在员工可以更改配置后重试。"
      : "Check again when the employee’s configuration can be changed.",
  };
  const reason = row.stale ? "stale" : row.adopt_blocked_reason;
  const policy = row.effective_work_policy;
  const time = (value: string) => new Date(value).toLocaleString(locale);
  const priority = { low: "低", normal: "普通", high: "高", urgent: "紧急" }[
    policy.default_priority
  ];
  return (
    <article className="space-y-2 rounded-md border p-3">
      <h4 className="font-medium">
        {zh ? "跟踪任务" : "Tracked assignments"} · {row.employee_name}
      </h4>
      <p>
        {zh ? "提议时的设置" : "At proposal time"} · {onOff(row.base_value)}
      </p>
      <p>
        {zh ? "提议的设置" : "Proposed setting"} · {onOff(row.proposed_value)}
      </p>
      {row.current_value !== row.base_value && (
        <p>
          {zh ? "当前设置" : "Current setting"} · {onOff(row.current_value)}
        </p>
      )}
      <p className="text-muted-foreground text-sm">{time(row.created_at)}</p>
      <dl className="grid gap-1 text-sm">
        <dt>
          {row.current_revision === row.base_revision
            ? zh
              ? "提议所用版本（当前版本）"
              : "Proposed against the current revision"
            : zh
              ? "提议所用版本"
              : "Proposed against revision"}
        </dt>
        <dd className="break-all">{row.base_revision}</dd>
        {row.current_revision !== row.base_revision && (
          <>
            <dt>{zh ? "当前版本" : "Current revision"}</dt>
            <dd className="break-all">{row.current_revision}</dd>
          </>
        )}
      </dl>
      {row.proposed_value ? (
        <div className="space-y-1 text-sm">
          <p>
            {zh
              ? "启用后允许创建新的跟踪任务。采纳提议本身不会创建任务。"
              : "Enabling allows new tracked assignments. Adopting this change creates no assignment."}
          </p>
          <p>
            {t.agentWork.priority} · {zh ? priority : policy.default_priority}
          </p>
          <p>
            {t.agentWork.reviewRequired} · {onOff(policy.review_required)}
          </p>
          <p>
            {zh ? "派生任务" : "Follow-up assignments"} ·{" "}
            {onOff(policy.allow_derived)}
            {policy.allow_derived &&
              (zh
                ? `（每次最多 ${policy.max_derived_per_activation} 项）`
                : ` (up to ${policy.max_derived_per_activation} per activation)`)}
          </p>
          {policy.responsibilities.length > 0 && (
            <p>
              {t.agentWork.responsibilities} ·{" "}
              {policy.responsibilities
                .map((entry) => entry.label)
                .join(zh ? "、" : ", ")}
            </p>
          )}
        </div>
      ) : (
        <p>
          {zh
            ? "关闭后无法创建新的跟踪任务。已有任务会保留。"
            : "Disabling prevents new tracked assignments. Existing assignments are retained."}
        </p>
      )}
      <p role="status">{status}</p>
      {row.decided_at && (
        <p className="text-sm">
          {time(row.decided_at)}{" "}
          {row.decided_by && <PersonName id={row.decided_by} />}
        </p>
      )}
      {row.resolved_by && row.resolved_by !== row.decided_by && (
        <p>
          {zh ? "处理人" : "Resolved by"} <PersonName id={row.resolved_by} />
        </p>
      )}
      {row.status === "open" && (
        <>
          {reason && <p>{reasons[reason]}</p>}
          {canDecide && (
            <div className="flex flex-wrap gap-2">
              {!reason && (
                <Button
                  disabled={busy || adoptBusy}
                  onClick={() => decide("adopt")}
                >
                  {zh ? "采纳更改" : "Adopt change"}
                </Button>
              )}
              <Button
                variant="outline"
                disabled={busy}
                onClick={() => decide("decline")}
              >
                {zh ? "拒绝" : "Decline"}
              </Button>
            </div>
          )}
        </>
      )}
    </article>
  );
}
