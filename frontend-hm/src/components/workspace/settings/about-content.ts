import { APP_VERSION } from "@/version";

/**
 * What About says: whose workspace this is -- the tenant's company when the
 * bundle names one, else the deployment's product name -- and the version
 * someone supporting them will ask for. Nothing else: no framework story, no
 * links elsewhere.
 */
export function aboutMarkdown(
  name: string,
  version: string = APP_VERSION,
): string {
  return `# ${escapeMarkdown(name)}\n\nWorkspace version ${version}.\n`;
}

/** The name as typed: a `#`, `*`, `_` or `[` in it is part of the name, not markup. */
function escapeMarkdown(text: string): string {
  return text.replace(/[\\`*_[\]#<>|~]/g, "\\$&");
}
