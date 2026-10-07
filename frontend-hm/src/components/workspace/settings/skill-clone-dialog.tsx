"use client";

import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";
import { previewSkillClone, type SkillClonePreview } from "@/core/skills/api";
import { useCloneSkill, useSkillCloneSources } from "@/core/skills/hooks";

export function SkillCloneDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const sources = useSkillCloneSources(open);
  const clone = useCloneSkill();
  const [sourceId, setSourceId] = useState("");
  const [name, setName] = useState("");
  const [override, setOverride] = useState(false);
  const [preview, setPreview] = useState<SkillClonePreview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const requestId = useRef(0);
  const controller = useRef<AbortController | null>(null);

  useEffect(() => {
    requestId.current += 1;
    controller.current?.abort();
    setSourceId("");
    setName("");
    setOverride(false);
    setPreview(null);
    setLoading(false);
    setError(null);
    return () => {
      requestId.current += 1;
      controller.current?.abort();
    };
  }, [open, user?.id, user?.system_role]);

  const source = sources.data?.find((item) => item.source_id === sourceId);
  const sameName = sources.data?.some((item) => item.name === name) === true;
  const canCopy =
    !sources.isError &&
    source?.enabled === true &&
    preview?.can_export === true &&
    typeof preview.revision === "string" &&
    !loading &&
    !clone.isPending &&
    /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(name) &&
    name.length <= 64 &&
    (!sameName || override);

  const selectSource = async (identifier: string) => {
    controller.current?.abort();
    const active = new AbortController();
    controller.current = active;
    const id = ++requestId.current;
    const selected = sources.data?.find(
      (item) => item.source_id === identifier,
    );
    setSourceId(identifier);
    setName(
      selected
        ? `${selected.name.slice(0, 56).replace(/-+$/, "")}-private`
        : "",
    );
    setOverride(false);
    setPreview(null);
    setError(null);
    if (!selected) {
      setLoading(false);
      return;
    }
    setLoading(true);
    try {
      const result = await previewSkillClone(identifier, active.signal);
      if (id === requestId.current) setPreview(result);
    } catch (error) {
      if (id === requestId.current && !active.signal.aborted)
        setError(error instanceof Error ? error.message : String(error));
    } finally {
      if (id === requestId.current) setLoading(false);
    }
  };

  const copy = async () => {
    if (!canCopy || !preview?.revision) return;
    const id = requestId.current;
    try {
      await clone.mutateAsync({
        source_id: sourceId,
        name,
        expected_revision: preview.revision,
        allow_baseline_override: sameName && override,
      });
      if (id === requestId.current) {
        toast.success(t.settings.skills.copySuccess);
        onOpenChange(false);
      }
    } catch (error) {
      if (id === requestId.current)
        setError(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t.settings.skills.cloneTitle}</DialogTitle>
          <DialogDescription>
            {t.settings.skills.cloneDescription}
          </DialogDescription>
        </DialogHeader>
        <label className="grid gap-2">
          {t.settings.skills.sourceLabel}
          <select
            className="border-input rounded-md border p-2"
            value={sourceId}
            disabled={clone.isPending || sources.isLoading}
            onChange={(event) => void selectSource(event.currentTarget.value)}
          >
            <option value="">{t.settings.skills.sourcePlaceholder}</option>
            {(sources.isError ? [] : (sources.data ?? [])).map((item) => (
              <option
                key={item.source_id}
                value={item.source_id}
                disabled={!item.enabled}
              >
                {item.name} ({item.category})
              </option>
            ))}
          </select>
        </label>
        <label className="grid gap-2">
          {t.settings.skills.nameLabel}
          <Input
            value={name}
            maxLength={64}
            disabled={!source || clone.isPending}
            onChange={(event) => {
              setName(event.currentTarget.value);
              setOverride(false);
            }}
          />
        </label>
        {sameName && (
          <label className="flex items-start gap-2">
            <input
              aria-label={t.settings.skills.overrideLabel}
              type="checkbox"
              checked={override}
              onChange={(event) => setOverride(event.currentTarget.checked)}
              disabled={clone.isPending}
            />
            <span>
              {t.settings.skills.overrideLabel}
              <span className="text-muted-foreground mt-1 block text-sm">
                {t.settings.skills.overrideDescription}
              </span>
            </span>
          </label>
        )}
        {loading && <p role="status">{t.common.loading}</p>}
        {preview && (
          <p>
            {t.settings.skills.previewFiles.replace(
              "{count}",
              String(preview.file_count),
            )}
          </p>
        )}
        {(error ?? sources.error) && (
          <>
            <p role="alert">
              {error ??
                (sources.error instanceof Error
                  ? sources.error.message
                  : String(sources.error))}
            </p>
            <Button
              variant="outline"
              disabled={loading || clone.isPending}
              onClick={() => {
                if (sources.isError) void sources.refetch();
                else void selectSource(sourceId);
              }}
            >
              {t.extensions.reload}
            </Button>
          </>
        )}
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t.common.cancel}
          </Button>
          <Button disabled={!canCopy} onClick={() => void copy()}>
            {clone.isPending
              ? t.settings.skills.copyPending
              : t.settings.skills.copyAction}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
