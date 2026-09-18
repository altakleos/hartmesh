import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "@rstest/core";

import { HISTORY_VALUE_KEYS } from "../../../e2e/utils/mock-api";

/**
 * The app renders four things from thread state, and a client that merely
 * *opens* a conversation never sees a `values` stream frame — its one state
 * read is `POST .../history`, whose `values` is a narrow projection.
 *
 * For a release the mocked suite answered that read with keys the Gateway did
 * not send, so the e2e asserted a wire contract production did not honour and
 * every reopened chat quietly lost its artifact list.
 * These two checks are what makes that shape a contract instead of a habit: the
 * keys the app depends on have to be in the projection, and the mock is not
 * allowed to invent one outside it.
 */

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, "..", "..", "..", "..");

function read(relative: string): string {
  return readFileSync(join(REPO, relative), "utf-8");
}

describe("the thread-state read a reopened chat makes", () => {
  it("names every key this app renders from thread state", () => {
    // The app's own declaration of what it draws from thread state.
    const streamState = read("src/core/threads/stream-state.ts");
    const declared = /const RENDERED_THREAD_STATE_KEYS = \[([^\]]*)\]/.exec(
      streamState,
    );
    expect(
      declared,
      "RENDERED_THREAD_STATE_KEYS moved; this check cannot see what the app renders",
    ).not.toBeNull();

    const rendered = [...declared![1]!.matchAll(/"([^"]+)"/g)].map(
      (match) => match[1]!,
    );
    expect(rendered.length).toBeGreaterThan(0);

    for (const key of rendered) {
      expect(
        HISTORY_VALUE_KEYS as readonly string[],
        `the app renders thread.values.${key}, so /history has to return it or it is blank on every reopened chat`,
      ).toContain(key);
    }
  });

  it("is not exceeded by the mocked backend", () => {
    // The mock's history handler, read as text: a key it answers with that the
    // Gateway cannot send is a green test over a broken product.
    const mock = read("tests/e2e/utils/mock-api.ts");
    const handler = mock.slice(
      mock.indexOf('void page.route("**/api/langgraph/threads/*/history"'),
      mock.indexOf("// Thread state — getState for individual thread"),
    );
    expect(handler.length).toBeGreaterThan(0);

    // Only the top-level keys of the `values` object: walk it and record an
    // identifier before a colon whenever nothing else is open, so the keys
    // inside the mocked message objects are not read as state channels.
    const start = handler.indexOf("values: {") + "values: ".length;
    const offered: string[] = [];
    let depth = 0;
    let token = "";
    for (let at = start; at < handler.length; at += 1) {
      const character = handler[at]!;
      if (character === "{" || character === "[") {
        depth += 1;
        token = "";
        continue;
      }
      if (character === "}" || character === "]") {
        depth -= 1;
        token = "";
        if (depth === 0) break;
        continue;
      }
      if (depth === 1 && character === ":" && token) {
        offered.push(token);
        token = "";
        continue;
      }
      token = /\w/.test(character) ? token + character : "";
    }

    expect(offered.length).toBeGreaterThan(0);

    for (const key of offered) {
      expect(
        HISTORY_VALUE_KEYS as readonly string[],
        `the mocked /history answers with "${key}", which the Gateway's projection does not return`,
      ).toContain(key);
    }
  });
});
