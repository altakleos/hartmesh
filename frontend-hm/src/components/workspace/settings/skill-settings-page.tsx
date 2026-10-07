"use client";

import { LoaderIcon, SparklesIcon, UploadIcon } from "lucide-react";
import { useRouter } from "next/navigation";
import { type ChangeEvent, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import {
  Item,
  ItemActions,
  ItemTitle,
  ItemContent,
  ItemDescription,
} from "@/components/ui/item";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAuth } from "@/core/auth/AuthProvider";
import { useCustomerAdministration } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import {
  formatSkillSecurityFindings,
  MAX_SKILL_ARCHIVE_UPLOAD_BYTES,
  SkillRequestError,
} from "@/core/skills/api";
import {
  useEnableSkill,
  useSkills,
  useUploadSkillArchive,
} from "@/core/skills/hooks";
import type { Skill } from "@/core/skills/type";
import { env } from "@/env";

import { ProviderEnablementNotice } from "./provider-enablement-notice";
import { SettingsSection } from "./settings-section";
import { SkillCloneDialog } from "./skill-clone-dialog";

export function SkillSettingsPage({ onClose }: { onClose?: () => void } = {}) {
  const { t } = useI18n();
  const { user } = useAuth();
  const { skills, isLoading, error } = useSkills();
  const adminRequired =
    error instanceof SkillRequestError && error.isAdminRequired;
  return (
    <SettingsSection
      title={t.settings.skills.title}
      description={t.settings.skills.description}
    >
      {isLoading ? (
        <div className="text-muted-foreground text-sm">{t.common.loading}</div>
      ) : adminRequired ? (
        <div className="text-muted-foreground text-sm">
          {t.settings.skills.adminRequired}
        </div>
      ) : error ? (
        <div>Error: {error.message}</div>
      ) : (
        <SkillSettingsList
          key={`${user?.id ?? ""}:${user?.system_role ?? ""}`}
          skills={skills}
          onClose={onClose}
        />
      )}
    </SettingsSection>
  );
}

function SkillSettingsList({
  skills,
  onClose,
}: {
  skills: Skill[];
  onClose?: () => void;
}) {
  const { t } = useI18n();
  const router = useRouter();
  const { localSkillManagement: canManageSkills } = useCustomerAdministration();
  const [filter, setFilter] = useState<string>("public");
  const [cloneOpen, setCloneOpen] = useState(false);
  const [archiveOverride, setArchiveOverride] = useState(false);
  const { mutate: enableSkill } = useEnableSkill();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { mutateAsync: uploadSkillArchive, isPending: isUploading } =
    useUploadSkillArchive();
  const staticReadOnly = env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true";
  const uploadLifetime = useRef(0);
  useEffect(() => {
    uploadLifetime.current += 1;
    return () => {
      uploadLifetime.current += 1;
    };
  }, [canManageSkills, staticReadOnly]);
  const isArchiveUploadDisabled =
    isUploading || !canManageSkills || staticReadOnly;
  const isCreateSkillDisabled = staticReadOnly;
  const filteredSkills = useMemo(
    () =>
      skills.filter((skill) =>
        filter === "public"
          ? skill.category !== "custom"
          : skill.category === "custom",
      ),
    [skills, filter],
  );
  const handleCreateSkill = () => {
    onClose?.();
    router.push("/workspace/chats/new?mode=skill");
  };
  const handleSkillArchive = async (event: ChangeEvent<HTMLInputElement>) => {
    if (isArchiveUploadDisabled) {
      event.target.value = "";
      return;
    }
    const archive = event.target.files?.[0];
    event.target.value = "";
    if (!archive) return;
    if (!archive.name.toLowerCase().endsWith(".skill")) {
      toast.error(t.settings.skills.invalidArchive);
      return;
    }
    if (archive.size > MAX_SKILL_ARCHIVE_UPLOAD_BYTES) {
      toast.error(t.settings.skills.archiveTooLarge);
      return;
    }

    const lifetime = uploadLifetime.current;
    try {
      const result = await uploadSkillArchive(
        archiveOverride ? { archive, allowBaselineOverride: true } : archive,
      );
      if (lifetime !== uploadLifetime.current) return;
      if (result.success) {
        toast.success(result.message);
        setFilter("custom");
        setArchiveOverride(false);
      } else {
        toast.error(result.message || t.settings.skills.installFailed);
      }
    } catch (error) {
      if (lifetime !== uploadLifetime.current) return;
      if (error instanceof SkillRequestError && error.isAdminRequired) {
        toast.error(t.settings.skills.installAdminRequired);
      } else if (error instanceof SkillRequestError && error.status === 413) {
        toast.error(t.settings.skills.archiveTooLarge);
      } else if (
        error instanceof SkillRequestError &&
        error.findings.length > 0
      ) {
        toast.error(error.message, {
          description: (
            <span className="whitespace-pre-line">
              {formatSkillSecurityFindings(error.findings)}
            </span>
          ),
        });
      } else {
        toast.error(
          error instanceof Error
            ? error.message
            : t.settings.skills.installFailed,
        );
      }
    }
  };
  return (
    <div className="flex w-full flex-col gap-4">
      {!canManageSkills && <ProviderEnablementNotice />}
      {canManageSkills && !staticReadOnly && (
        <SkillCloneDialog open={cloneOpen} onOpenChange={setCloneOpen} />
      )}
      <header className="flex flex-wrap justify-between gap-2">
        <div className="flex gap-2">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList variant="line">
              <TabsTrigger value="public">
                {t.settings.skills.provided}
              </TabsTrigger>
              <TabsTrigger value="custom">
                {t.settings.skills.privateSkills}
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
        <div className="flex gap-2">
          <input
            ref={fileInputRef}
            type="file"
            accept=".skill"
            disabled={isArchiveUploadDisabled}
            className="sr-only"
            onChange={handleSkillArchive}
          />
          {canManageSkills && (
            <Button
              size="sm"
              variant="outline"
              disabled={staticReadOnly}
              onClick={() => setCloneOpen(true)}
            >
              {t.settings.skills.cloneProvided}
            </Button>
          )}
          {canManageSkills && (
            <Button
              size="sm"
              variant="outline"
              disabled={isArchiveUploadDisabled}
              onClick={() => fileInputRef.current?.click()}
            >
              {isUploading ? (
                <LoaderIcon className="size-4 animate-spin" />
              ) : (
                <UploadIcon className="size-4" />
              )}
              {isUploading
                ? t.settings.skills.installingArchive
                : t.settings.skills.installFromFile}
            </Button>
          )}
          <Button
            size="sm"
            disabled={isCreateSkillDisabled}
            onClick={handleCreateSkill}
          >
            <SparklesIcon className="size-4" />
            {t.settings.skills.createSkill}
          </Button>
        </div>
      </header>
      {canManageSkills && !staticReadOnly && (
        <label className="flex items-start gap-2 text-sm">
          <input
            type="checkbox"
            checked={archiveOverride}
            disabled={isUploading}
            onChange={(event) =>
              setArchiveOverride(event.currentTarget.checked)
            }
          />
          <span>{t.settings.skills.archiveOverride}</span>
        </label>
      )}
      {filteredSkills.length === 0 && (
        <EmptySkill
          createDisabled={isCreateSkillDisabled}
          onCreateSkill={handleCreateSkill}
        />
      )}
      {filteredSkills.length > 0 &&
        filteredSkills.map((skill) => (
          <Item className="w-full" variant="outline" key={skill.name}>
            <ItemContent>
              <ItemTitle>
                <div className="flex items-center gap-2">{skill.name}</div>
              </ItemTitle>
              {skill.origin && (
                <p className="text-muted-foreground text-xs">
                  {t.settings.skills.privateOrigin.replace(
                    "{name}",
                    skill.origin.source_name,
                  )}
                  <span className="ml-2" title={skill.origin.revision}>
                    {skill.origin.source_category} ·{" "}
                    {skill.origin.revision.slice(0, 12)}
                  </span>
                </p>
              )}
              {skill.overrides_baseline && (
                <p className="text-muted-foreground text-xs">
                  {t.settings.skills.baselineOverride}
                </p>
              )}
              <ItemDescription className="line-clamp-4">
                {skill.description}
              </ItemDescription>
            </ItemContent>
            <ItemActions>
              <Switch
                checked={skill.enabled}
                disabled={
                  staticReadOnly ||
                  !canManageSkills ||
                  skill.category === "public"
                }
                onCheckedChange={(checked) =>
                  enableSkill({ skillName: skill.name, enabled: checked })
                }
              />
            </ItemActions>
          </Item>
        ))}
    </div>
  );
}

function EmptySkill({
  createDisabled,
  onCreateSkill,
}: {
  createDisabled: boolean;
  onCreateSkill: () => void;
}) {
  const { t } = useI18n();
  return (
    <Empty>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <SparklesIcon />
        </EmptyMedia>
        <EmptyTitle>{t.settings.skills.emptyTitle}</EmptyTitle>
        <EmptyDescription>
          {t.settings.skills.emptyDescription}
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Button disabled={createDisabled} onClick={onCreateSkill}>
          {t.settings.skills.emptyButton}
        </Button>
      </EmptyContent>
    </Empty>
  );
}
