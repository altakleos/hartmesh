"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useId, useMemo } from "react";

import { usePromptInputController } from "@/components/ai-elements/prompt-input";
import { useWorkspacePresentation } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";

import { AuroraText } from "../ui/aurora-text";

import { useComposerFocus } from "./composer-focus";

let waved = false;

function WelcomeDescription({ children }: { children: string }) {
  return (
    <p className="max-w-full text-wrap break-words whitespace-pre-line">
      {children}
    </p>
  );
}

/**
 * What Home offers before anyone types.
 *
 * Choosing one fills the message box and sends nothing: the first moment is
 * meant to be "pick the thing, drop the file, say the month", so the person
 * keeps the last word — and the cursor, which is why this focuses the box
 * afterwards. The list is the deployment's, from `ui.starters`.
 */
function Starters({ className }: { className?: string }) {
  const { t } = useI18n();
  // `Welcome` renders inside the composer's own provider, so a tile can put
  // its words in the box without any of this being plumbed through the page.
  const { textInput } = usePromptInputController();
  const focusComposer = useComposerFocus();
  const { starters } = useWorkspacePresentation();
  const hintId = useId();

  if (!starters?.length) {
    return null;
  }

  const fill = (prompt: string) => {
    // Replace what a starter put there; never what a person typed. Appending
    // means a tapped tile can always be undone by deleting one half, and
    // swapping between two tiles still reads as a swap.
    const current = textInput.value.trim();
    const isUntouched =
      current === "" ||
      starters.some((starter) => starter.prompt.trim() === current);
    textInput.setInput(
      isUntouched ? prompt : `${textInput.value.trimEnd()}\n\n${prompt}`,
    );
    focusComposer();
  };

  return (
    <div
      className={cn("flex max-w-full flex-col items-center gap-2", className)}
    >
      <p className="text-muted-foreground max-w-full text-sm" id={hintId}>
        {t.welcome.startersHint}
      </p>
      <div
        aria-labelledby={hintId}
        className="flex max-w-full flex-wrap items-center justify-center gap-2"
        data-testid="welcome-starters"
        role="group"
      >
        {starters.map((starter) => (
          <button
            className="border-border bg-background/60 hover:bg-accent hover:text-accent-foreground focus-visible:ring-ring max-w-full rounded-full border px-3.5 py-1.5 text-sm break-words transition-colors focus-visible:ring-2 focus-visible:outline-none"
            key={starter.id}
            onClick={() => fill(starter.prompt)}
            type="button"
          >
            {starter.title}
          </button>
        ))}
      </div>
    </div>
  );
}

export function Welcome({
  className,
  mode,
}: {
  className?: string;
  mode?: "ultra" | "pro" | "thinking" | "flash";
}) {
  const { t } = useI18n();
  const searchParams = useSearchParams();
  const { profile, isLoading } = useWorkspacePresentation();
  const isUltra = useMemo(() => mode === "ultra", [mode]);
  const colors = useMemo(() => {
    if (isUltra) {
      return ["#efefbb", "#e9c665", "#e3a812"];
    }
    return ["var(--color-foreground)"];
  }, [isUltra]);
  useEffect(() => {
    waved = true;
  }, []);
  const isSkillMode = searchParams.get("mode") === "skill";
  // One rule, the same one the starters already follow: a control someone
  // might need stays offered while the deployment's answer is unknown, and
  // copy the deployment authors waits for it. `=== "developer"` also covers a
  // failed fetch, where `isLoading` is false and `profile` is still undefined.
  const showsProductBlurb = !isLoading && profile === "developer";
  return (
    <div
      className={cn(
        "mx-auto flex w-full max-w-full flex-col items-center justify-center gap-2 px-4 py-4 text-center sm:px-8",
        className,
      )}
    >
      <div className="max-w-full text-2xl font-bold">
        {isSkillMode ? (
          `✨ ${t.welcome.createYourOwnSkill} ✨`
        ) : (
          <div className="flex max-w-full flex-wrap items-center justify-center gap-2">
            <div className={cn("inline-block", !waved ? "animate-wave" : "")}>
              {isUltra ? "🚀" : "👋"}
            </div>
            <AuroraText colors={colors}>{t.welcome.greeting}</AuroraText>
          </div>
        )}
      </div>
      {isSkillMode ? (
        <div className="text-muted-foreground max-w-full text-sm">
          <WelcomeDescription>
            {t.welcome.createYourOwnSkillDescription}
          </WelcomeDescription>
        </div>
      ) : (
        showsProductBlurb && (
          <div className="text-muted-foreground max-w-full text-sm">
            <WelcomeDescription>{t.welcome.description}</WelcomeDescription>
          </div>
        )
      )}
      {!isSkillMode && <Starters className="mt-1" />}
    </div>
  );
}
