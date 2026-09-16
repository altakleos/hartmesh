/**
 * Which settings screens a person is offered.
 *
 * Under `ui.profile: business` the screens that only make sense to someone
 * building the deployment are kept for administrators; everyone else keeps
 * their account, their channels, their memory, and how the app looks and
 * notifies them. Channels and memory are deliberately not in that set: both
 * are a person's own — what the agent remembers about them, and the phone
 * they message it from.
 *
 * Hiding them is presentation, not authorization — the routes behind them are
 * unchanged and `authorization` has no permission covering them; `system_role`
 * is what limits a person.
 */

export type SettingsSectionId =
  | "account"
  | "appearance"
  | "channels"
  | "integrations"
  | "memory"
  | "tools"
  | "subagents"
  | "skills"
  | "notification"
  | "about";

export const DEVELOPER_SETTINGS_SECTIONS: ReadonlySet<SettingsSectionId> =
  new Set(["integrations", "tools", "subagents", "skills"]);

export function visibleSettingsSections<T extends { id: string }>(
  sections: readonly T[],
  developerSurfacesVisible: boolean,
): T[] {
  if (developerSurfacesVisible) {
    return [...sections];
  }
  return sections.filter(
    (section) =>
      !DEVELOPER_SETTINGS_SECTIONS.has(section.id as SettingsSectionId),
  );
}

/**
 * The screen to show. A deep link to a hidden screen lands on the account
 * page rather than rendering the screen anyway.
 */
export function resolveActiveSection<T extends { id: string }>(
  visible: readonly T[],
  active: SettingsSectionId,
): SettingsSectionId {
  return visible.some((section) => section.id === active) ? active : "account";
}
