import type { AgentTarget } from "./web-pages";

const RANDOM_CHARACTERS = "abcdefghjkmnpqrstuvwxyz23456789";
const NAME_MAX = 64;
const NAME_HEAD = 62;
const SLUG_MAX = 20;

function randomCharacters(length: number): string {
  const values = crypto.getRandomValues(new Uint32Array(length));
  return Array.from(
    values,
    (value) => RANDOM_CHARACTERS[value % RANDOM_CHARACTERS.length],
  ).join("");
}

/** The same rule the server follows for a bot it creates. */
export function suggestUsername(target: AgentTarget): string {
  if (target.kind === "chat") {
    return `chat_${randomCharacters(6)}_bot`;
  }
  const slug = target.name
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .slice(0, SLUG_MAX);
  if (!slug) {
    return `ai_${randomCharacters(8)}_bot`;
  }
  const prefix = /^[a-z]/.test(slug) ? "" : "ai_";
  return `${prefix}${slug}_${randomCharacters(4)}_bot`;
}

/** A target's name as a bot's display name, counted in code points as the server does. */
export function displayNameOf(name: string): string {
  const characters = Array.from(name);
  if (characters.length <= NAME_MAX) {
    return name;
  }
  const head = characters.slice(0, NAME_HEAD);
  const space = head.lastIndexOf(" ");
  return `${(space === -1 ? head : head.slice(0, space)).join("")}…`;
}
