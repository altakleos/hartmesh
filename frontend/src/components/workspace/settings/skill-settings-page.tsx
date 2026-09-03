"use client";

import { LoaderIcon, SparklesIcon, UploadIcon } from "lucide-react";
import { useRouter } from "next/navigation";
import { type ChangeEvent, useMemo, useRef, useState } from "react";
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
import { useToolPlaneGovernance } from "@/core/tool-plane";
import { env } from "@/env";

import { SettingsSection } from "./settings-section";
import { ToolPlaneGovernanceNotice } from "./tool-plane-governance-notice";

export function SkillSettingsPage({ onClose }: { onClose?: () => void } = {}) {
  const { t } = useI18n();
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
        <SkillSettingsList skills={skills} onClose={onClose} />
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
  const { user } = useAuth();
  const isAdmin = user?.system_role === "admin";
  const [filter, setFilter] = useState<string>("public");
  const baseToolPlane = useToolPlaneGovernance("deployment_base", isAdmin);
  const overlayToolPlane = useToolPlaneGovernance("user_overlay");
  const toolPlane = filter === "public" ? baseToolPlane : overlayToolPlane;
  const { mutate: enableSkill } = useEnableSkill();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { mutateAsync: uploadSkillArchive, isPending: isUploading } =
    useUploadSkillArchive();
  const staticReadOnly = env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true";
  const isArchiveUploadDisabled =
    isUploading ||
    !isAdmin ||
    staticReadOnly ||
    overlayToolPlane.legacyMutationBlocked;
  const isCreateSkillDisabled =
    staticReadOnly || overlayToolPlane.legacyMutationBlocked;
  const filteredSkills = useMemo(
    () => skills.filter((skill) => skill.category === filter),
    [skills, filter],
  );
  const handleCreateSkill = () => {
    onClose?.();
    router.push("/workspace/chats/new?mode=skill");
  };
  const handleSkillArchive = async (event: ChangeEvent<HTMLInputElement>) => {
    if (isUploading) {
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

    try {
      const result = await uploadSkillArchive(archive);
      if (result.success) {
        toast.success(result.message);
        setFilter("custom");
      } else {
        toast.error(result.message || t.settings.skills.installFailed);
      }
    } catch (error) {
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
      {(filter === "custom" || isAdmin) && (
        <ToolPlaneGovernanceNotice {...toolPlane} />
      )}
      <header className="flex justify-between">
        <div className="flex gap-2">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList variant="line">
              <TabsTrigger value="public">{t.common.public}</TabsTrigger>
              <TabsTrigger value="custom">{t.common.custom}</TabsTrigger>
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
          {isAdmin && (
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
              <ItemDescription className="line-clamp-4">
                {skill.description}
              </ItemDescription>
            </ItemContent>
            <ItemActions>
              <Switch
                checked={skill.enabled}
                disabled={
                  staticReadOnly || !isAdmin || toolPlane.legacyMutationBlocked
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
