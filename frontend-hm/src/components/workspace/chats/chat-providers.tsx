"use client";

import { PromptInputProvider } from "@/components/ai-elements/prompt-input";
import { ArtifactsProvider } from "@/components/workspace/artifacts";
import { BrowserViewProvider } from "@/components/workspace/browser-view";
import { ArtifactDeliveryProvider } from "@/core/artifact-delivery";
import { SubtasksProvider } from "@/core/tasks/context";
import { TurnProgressProvider } from "@/core/turn-progress";

export function ChatProviders({ children }: { children: React.ReactNode }) {
  return (
    <SubtasksProvider>
      <ArtifactDeliveryProvider>
        <TurnProgressProvider>
          <ArtifactsProvider>
            <BrowserViewProvider>
              <PromptInputProvider>{children}</PromptInputProvider>
            </BrowserViewProvider>
          </ArtifactsProvider>
        </TurnProgressProvider>
      </ArtifactDeliveryProvider>
    </SubtasksProvider>
  );
}
