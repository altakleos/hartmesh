"use client";

import {
  BellIcon,
  CableIcon,
  InfoIcon,
  BrainIcon,
  PaletteIcon,
  PlugZapIcon,
  SparklesIcon,
  UsersRoundIcon,
  UserIcon,
  WrenchIcon,
} from "lucide-react";
import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  useDeveloperSurfacesVisible,
  useWorkspacePresentation,
} from "@/core/features";
import { useBranding } from "@/core/features";
import { useI18n } from "@/core/i18n/hooks";
import { cn } from "@/lib/utils";

import {
  resolveActiveSection,
  visibleSettingsSections,
  type SettingsSectionId,
} from "./settings-sections";

function SettingsPageLoading() {
  return (
    <p role="status" className="text-muted-foreground py-8 text-center text-sm">
      Loading…
    </p>
  );
}

const AccountSettingsPage = dynamic(
  () =>
    import("./account-settings-page").then(
      (module) => module.AccountSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const AppearanceSettingsPage = dynamic(
  () =>
    import("./appearance-settings-page").then(
      (module) => module.AppearanceSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const ChannelsSettingsPage = dynamic(
  () =>
    import("./channels-settings-page").then(
      (module) => module.ChannelsSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const IntegrationsSettingsPage = dynamic(
  () =>
    import("./integrations-settings-page").then(
      (module) => module.IntegrationsSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const MemorySettingsPage = dynamic(
  () =>
    import("./memory-settings-page").then(
      (module) => module.MemorySettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const NotificationSettingsPage = dynamic(
  () =>
    import("./notification-settings-page").then(
      (module) => module.NotificationSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const SkillSettingsPage = dynamic(
  () =>
    import("./skill-settings-page").then((module) => module.SkillSettingsPage),
  { loading: SettingsPageLoading },
);
const ToolSettingsPage = dynamic(
  () =>
    import("./tool-settings-page").then((module) => module.ToolSettingsPage),
  { loading: SettingsPageLoading },
);
const SubagentSettingsPage = dynamic(
  () =>
    import("./subagent-settings-page").then(
      (module) => module.SubagentSettingsPage,
    ),
  { loading: SettingsPageLoading },
);
const AboutSettingsPage = dynamic(
  () =>
    import("./about-settings-page").then((module) => module.AboutSettingsPage),
  { loading: SettingsPageLoading },
);

type SettingsDialogProps = React.ComponentProps<typeof Dialog> & {
  defaultSection?: SettingsSectionId;
};

export function SettingsDialog(props: SettingsDialogProps) {
  const { defaultSection = "appearance", ...dialogProps } = props;
  const { t } = useI18n();
  const { companyName } = useBranding();
  const developerSurfacesVisible = useDeveloperSurfacesVisible();
  // The screens stay offered while the deployment's answer is unknown, so a
  // deep link to a hidden one would otherwise mount it — and fire its
  // fetches — for the moment before the answer lands.
  const { isLoading: presentationLoading } = useWorkspacePresentation();
  const [activeSection, setActiveSection] =
    useState<SettingsSectionId>(defaultSection);

  useEffect(() => {
    // When opening the dialog, ensure the active section follows the caller's intent.
    // This allows triggers like "About" to open the dialog directly on that page.
    if (dialogProps.open) {
      setActiveSection(defaultSection);
    }
  }, [defaultSection, dialogProps.open]);

  const sections = useMemo(
    () => [
      {
        id: "account",
        label: t.settings.sections.account,
        icon: UserIcon,
      },
      {
        id: "appearance",
        label: t.settings.sections.appearance,
        icon: PaletteIcon,
      },
      {
        id: "notification",
        label: t.settings.sections.notification,
        icon: BellIcon,
      },
      {
        id: "channels",
        label: t.settings.sections.channels,
        icon: CableIcon,
      },
      {
        id: "integrations",
        label: t.settings.sections.integrations,
        icon: PlugZapIcon,
      },
      {
        id: "memory",
        label: t.settings.sections.memory,
        icon: BrainIcon,
      },
      { id: "tools", label: t.settings.sections.tools, icon: WrenchIcon },
      {
        id: "subagents",
        label: t.settings.sections.subagents,
        icon: UsersRoundIcon,
      },
      { id: "skills", label: t.settings.sections.skills, icon: SparklesIcon },
      { id: "about", label: t.settings.sections.about, icon: InfoIcon },
    ],
    [
      t.settings.sections.account,
      t.settings.sections.appearance,
      t.settings.sections.channels,
      t.settings.sections.integrations,
      t.settings.sections.memory,
      t.settings.sections.tools,
      t.settings.sections.subagents,
      t.settings.sections.skills,
      t.settings.sections.notification,
      t.settings.sections.about,
    ],
  );
  const visibleSections = useMemo(
    () => visibleSettingsSections(sections, developerSurfacesVisible),
    [developerSurfacesVisible, sections],
  );
  const activeVisibleSection = resolveActiveSection(
    visibleSections,
    activeSection,
  );
  return (
    <Dialog
      {...dialogProps}
      onOpenChange={(open) => props.onOpenChange?.(open)}
    >
      <DialogContent
        className="flex h-[75vh] max-h-[calc(100vh-2rem)] flex-col sm:max-w-5xl md:max-w-6xl"
        aria-describedby={undefined}
      >
        <DialogHeader className="gap-1">
          <DialogTitle>{t.settings.title}</DialogTitle>
          <p className="text-muted-foreground text-sm">
            {companyName === null
              ? t.settings.description
              : t.settings.descriptionForCompany}
          </p>
        </DialogHeader>
        <div className="grid min-h-0 flex-1 gap-4 md:grid-cols-[220px_minmax(0,1fr)]">
          <nav className="bg-sidebar min-h-0 overflow-y-auto rounded-lg border p-2">
            <ul className="space-y-1 pr-1">
              {visibleSections.map(({ id, label, icon: Icon }) => {
                const active = activeVisibleSection === id;
                return (
                  <li key={id}>
                    <button
                      type="button"
                      onClick={() => setActiveSection(id as SettingsSectionId)}
                      className={cn(
                        "flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                        active
                          ? "bg-primary text-primary-foreground shadow-sm"
                          : "text-muted-foreground hover:bg-muted hover:text-foreground",
                      )}
                    >
                      <Icon className="size-4" />
                      <span>{label}</span>
                    </button>
                  </li>
                );
              })}
            </ul>
          </nav>
          <ScrollArea className="h-full min-h-0 rounded-lg border">
            <div className="space-y-8 p-6">
              {presentationLoading && <SettingsPageLoading />}
              {!presentationLoading && (
                <>
                  {activeVisibleSection === "account" && (
                    <AccountSettingsPage />
                  )}
                  {activeVisibleSection === "appearance" && (
                    <AppearanceSettingsPage />
                  )}
                  {activeVisibleSection === "memory" && <MemorySettingsPage />}
                  {activeVisibleSection === "tools" && <ToolSettingsPage />}
                  {activeVisibleSection === "subagents" && (
                    <SubagentSettingsPage />
                  )}
                  {activeVisibleSection === "skills" && (
                    <SkillSettingsPage
                      onClose={() => props.onOpenChange?.(false)}
                    />
                  )}
                  {activeVisibleSection === "notification" && (
                    <NotificationSettingsPage />
                  )}
                  {activeVisibleSection === "channels" && (
                    <ChannelsSettingsPage />
                  )}
                  {activeVisibleSection === "integrations" && (
                    <IntegrationsSettingsPage />
                  )}
                  {activeVisibleSection === "about" && <AboutSettingsPage />}
                </>
              )}
            </div>
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  );
}
