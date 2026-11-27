"use client";

import { useSearchParams } from "next/navigation";

import { useWorkspacePresentation } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";

export function Welcome({ className }: { className?: string }) {
  const { t } = useI18n();
  const searchParams = useSearchParams();
  const { profile, isLoading } = useWorkspacePresentation();
  const isSkillMode = searchParams.get("mode") === "skill";
  const showsProductBlurb = !isLoading && profile === "developer";

  if (!isSkillMode && !showsProductBlurb) {
    return null;
  }

  return (
    <div
      className={cn(
        "mx-auto flex w-full max-w-full flex-col items-center gap-2 px-4 py-4 text-center sm:px-8",
        className,
      )}
    >
      {isSkillMode && (
        <div className="max-w-full text-2xl font-bold">
          ✨ {t.welcome.createYourOwnSkill} ✨
        </div>
      )}
      <p className="text-muted-foreground max-w-full text-sm text-wrap break-words whitespace-pre-line">
        {isSkillMode
          ? t.welcome.createYourOwnSkillDescription
          : t.welcome.description}
      </p>
    </div>
  );
}
