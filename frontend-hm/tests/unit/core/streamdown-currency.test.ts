import { expect, test } from "@rstest/core";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SafeStreamdown, streamdownPlugins } from "@/core/streamdown";

// Renders through the chat preset exactly as a message bubble does, so these
// assertions cover the whole chain: preprocess -> remark-math -> rehype-katex.
//
// These tests deliberately assert the *render*, never textContent. The released
// capture of the defect had correct extracted DOM text ("$201,487.04 in revenue
// ...") while the pixels showed an equation, so a textContent assertion over a
// KaTeX render is a control that cannot fail. `class="katex"` is the only
// signal that distinguishes the two.
function renderChatMarkdown(content: string) {
  return renderToStaticMarkup(
    createElement(SafeStreamdown, streamdownPlugins, content),
  );
}

function expectNoMath(content: string) {
  const html = renderChatMarkdown(content);
  expect(html).not.toContain("katex");
  return html;
}

// `katexOptions` sets `throwOnError: false`, so a formula KaTeX cannot parse
// still emits `class="katex"` wrapped around red error glyphs. Asserting the
// class alone would therefore accept a broken render; `katex-error` has to be
// excluded for "still renders as mathematics" to mean it. `display` pins which
// of the two shapes came out, because `\(...\)` and `\[...\]` now normalize to
// the same `$$` delimiter and only position distinguishes them.
function expectMath(content: string, { display }: { display: boolean }) {
  const html = renderChatMarkdown(content);
  expect(html).toContain('class="katex"');
  expect(html).not.toContain("katex-error");
  if (display) {
    expect(html).toContain("katex-display");
  } else {
    expect(html).not.toContain("katex-display");
  }
  return html;
}

// ---------------------------------------------------------------------------
// Currency. Every case below renders as an equation without the fix.
// ---------------------------------------------------------------------------

test("a sentence with two currency amounts is not typeset as mathematics", () => {
  // The exact sentence measured on a released build, which rendered as
  // "August 2026: *201,487.04inrevenueacross502jobs,anaverageof*401.37 per job."
  const html = expectNoMath(
    "August 2026: $201,487.04 in revenue across 502 jobs, an average of $401.37 per job.",
  );

  // Both amounts keep their currency, and the words between them keep their
  // spaces — the two things the equation render destroyed.
  expect(html).toContain("$201,487.04 in revenue across 502 jobs");
  expect(html).toContain("$401.37 per job");
});

test("three currency amounts in a sentence leave no stray dollar sign", () => {
  // With single-dollar math on, the first two amounts became an equation and
  // the third `$` was left behind as literal text.
  const html = expectNoMath(
    "Revenue $201,487.04 and unpaid $29,881.04 and average $401.37.",
  );

  expect(html).toContain("$201,487.04");
  expect(html).toContain("$29,881.04");
  expect(html).toContain("$401.37");
});

test("currency amounts split across two sentences are not joined into math", () => {
  const html = expectNoMath(
    "B. Chen led with $35,548.68 across 93 jobs. $29,881.04 across 59 jobs is unpaid.",
  );

  expect(html).toContain("$35,548.68 across 93 jobs");
  expect(html).toContain("$29,881.04 across 59 jobs is unpaid");
});

test("whole-dollar amounts without commas or decimals are not typeset as math", () => {
  // Neither a comma, a decimal point nor a following space was what saved the
  // safe cases, so the bare form has to be covered too.
  const html = expectNoMath("Costs rose from $10 to $20.");
  expect(html).toContain("$10 to $20");
});

test("a zero amount alongside another amount stays plain", () => {
  const html = expectNoMath(
    "18 cancelled jobs and 35 jobs at $0.00 are included, worth $0.00 in revenue.",
  );
  expect(html).toContain("$0.00");
});

test("an amount inside parentheses alongside another amount stays plain", () => {
  const html = expectNoMath(
    "That is 6.7% above July 2026 ($188,836.77), up $12,650.27.",
  );
  expect(html).toContain("($188,836.77)");
  expect(html).toContain("$12,650.27");
});

// ---------------------------------------------------------------------------
// Money shapes that do not start with a digit after the `$`. These are the
// reason the rejected alternative — escaping a `$` that precedes a digit —
// would have removed only the measured instances rather than the class: none
// of these is digit-prefixed, and every one of them was math before the fix.
// ---------------------------------------------------------------------------

test("accounting negatives in parentheses are not typeset as math", () => {
  const html = expectNoMath("Adjustments were $(1,200) and $(430) this month.");
  expect(html).toContain("$(1,200)");
  expect(html).toContain("$(430)");
});

test("cents-only amounts are not typeset as math", () => {
  const html = expectNoMath("Fees were $.99 and $.50 per job.");
  expect(html).toContain("$.99");
  expect(html).toContain("$.50");
});

test("amounts written with a space after the dollar sign stay plain", () => {
  // A space after `$` does not stop remark-math opening a span, so this was
  // broken too.
  const html = expectNoMath("Totals were $ 1,200 and $ 400 this month.");
  expect(html).toContain("1,200");
  expect(html).toContain("400");
});

test("amounts on adjacent lines of one paragraph are not joined into math", () => {
  // A line ending is allowed inside a math span, so a soft-wrapped sentence
  // was affected exactly like a single-line one.
  const html = expectNoMath(
    "Revenue was $201,487.04\nand the average was $401.37.",
  );
  expect(html).toContain("$201,487.04");
  expect(html).toContain("$401.37");
});

// ---------------------------------------------------------------------------
// Block containers. A report puts figures in tables, lists and headings, and
// each of these was an equation before the fix.
// ---------------------------------------------------------------------------

test("two currency amounts in one table cell are not typeset as math", () => {
  const html = expectNoMath(
    [
      "| Month | Detail |",
      "| --- | --- |",
      "| August | $201,487.04 across 502 jobs, avg $401.37 |",
    ].join("\n"),
  );

  expect(html).toContain("$201,487.04 across 502 jobs");
  expect(html).toContain("$401.37");
});

test("one amount per table cell stays plain", () => {
  // Unlike the case above, this one was already safe before the fix: remark-gfm
  // gives each cell its own inline token stream, so two `$` in different cells
  // can never pair across the `|`. It is kept as a guard against a future
  // preprocessing step that joins cells, not as a regression test for this bug.
  const html = expectNoMath(
    [
      "| Month | Revenue | Average |",
      "| --- | --- | --- |",
      "| August | $201,487.04 | $401.37 |",
    ].join("\n"),
  );

  expect(html).toContain("$201,487.04");
  expect(html).toContain("$401.37");
});

test("two currency amounts in one list item are not typeset as math", () => {
  const html = expectNoMath(
    "- revenue $201,487.04 and average $401.37\n- 502 jobs",
  );
  expect(html).toContain("$201,487.04");
  expect(html).toContain("$401.37");
});

test("two currency amounts in a heading are not typeset as math", () => {
  const html = expectNoMath("## Revenue $201,487.04 and average $401.37");
  expect(html).toContain("$201,487.04");
  expect(html).toContain("$401.37");
});

test("bold currency amounts are not typeset as math", () => {
  const html = expectNoMath("**$201,487.04** in revenue, **$401.37** average.");
  expect(html).toContain("$201,487.04");
  expect(html).toContain("$401.37");
});

test("a lone currency amount in a sentence stays plain", () => {
  // A control: an unpaired `$` was never math. It guards against a fix that
  // strips or escapes dollar signs outright.
  const html = expectNoMath("Revenue was $201,487.04.");
  expect(html).toContain("$201,487.04");
});

// ---------------------------------------------------------------------------
// Mathematics must still render, and the accepted cost is pinned so that
// re-enabling the flag breaks a test that says why it is off.
// ---------------------------------------------------------------------------

test("a genuine double-dollar formula still renders as inline mathematics", () => {
  expectMath("The rate is $$r = 0.067$$ per month.", { display: false });
});

test("a display formula on its own lines still renders as display mathematics", () => {
  expectMath(["$$", "E=mc^2", "$$"].join("\n"), { display: true });
});

test("LaTeX inline delimiters still render as inline mathematics", () => {
  // Many models emit \(...\); preprocess rewrites the delimiters, and that
  // rewrite has to keep producing a form remark-math still recognizes — and
  // keep it inline, since it now emits the same `$$` display math uses.
  expectMath("Given \\(x^2\\), compute the area.", { display: false });
});

test("LaTeX display delimiters still render as display mathematics", () => {
  expectMath(["Before", "\\[", "x^2 + y^2 = z^2", "\\]", "After"].join("\n"), {
    display: true,
  });
});

// An inline span that crosses a line break is the shape that breaks if the
// `\(...\)` rewrite emits `$$` naively: at the start of a line `$$` opens a
// display fence instead of inline math.

test("an inline LaTeX span opening a line still renders as inline mathematics", () => {
  expectMath("\\(a\nb\\) is the sum.", { display: false });
});

test("an inline LaTeX span closing on its own line stays inline", () => {
  expectMath("Given \\(x\nsomething\n\\) done", { display: false });
});

test("an unclosed inline span does not consume a later display formula", () => {
  // The worst shape: an orphan `$$` left by the rewrite paired with the
  // *opening* `$$` of the next genuine block and destroyed it.
  const html = expectMath(
    ["\\(", "x\\)", "", "then", "", "$$", "y", "$$"].join("\n"),
    { display: true },
  );
  expect(html).toContain("then");
});

test("single-dollar inline math is intentionally gone", () => {
  // The accepted cost of keeping currency intact: a model that emits `$x$`
  // prints it literally. This is deliberate, not a defect — a bug report about
  // literal LaTeX must not be closed by turning singleDollarTextMath back on,
  // because that restores the currency defect for every business report.
  const html = expectNoMath("Let $x$ be a number.");
  expect(html).toContain("$x$");
});
