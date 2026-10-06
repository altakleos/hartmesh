import { parseBusinessReport, checksLine, cellFormat } from "./report.mjs";
import { formatValue } from "./format.mjs";
import { REPORTS_FOLDER, reportOfRender } from "./paths.mjs";

const labels = {
  en: {
    draft: (n) => `Draft ${n}`,
    compared: (period) => `vs ${period}`,
    checks: "Checks",
    omitted: "Not included",
    statuses: {
      pass: "ok",
      warn: "heads up",
      fail: "problem",
      not_checked: "not checked",
    },
    uploaded: (name, date) => `${name} (uploaded ${date})`,
    inputs: (names) =>
      `Built from ${names.join(", ")}. Checked by the report script.`,
  },
  zh: {
    draft: (n) => `第 ${n} 稿`,
    compared: (period) => `相比 ${period}`,
    checks: "核对",
    omitted: "未包含",
    statuses: {
      pass: "没问题",
      warn: "请注意",
      fail: "有问题",
      not_checked: "未核对",
    },
    uploaded: (name, date) => `${name}（上传于 ${date}）`,
    inputs: (names) => `根据 ${names.join("、")} 生成，已由报告脚本核对。`,
  },
};
const renders = [
  ["pdf", "PDF"],
  ["docx", "Word"],
  ["xlsx", "Excel"],
];
const glyphs = { pass: "✓", warn: "!", fail: "✕", not_checked: "–" };
const numeric = new Set(["currency", "integer", "number", "percent"]);
const css = `
:host{display:block;min-width:0;font-family:inherit;color:inherit}
*{box-sizing:border-box}article{max-width:48rem;margin:auto;padding:1.25rem 1rem;font-size:14px;line-height:1.5;overflow-wrap:break-word}
h2{margin:0;font-size:20px;line-height:1.25;font-weight:600;text-wrap:balance}h3{margin:0;border-bottom:1px solid var(--accent);padding-bottom:4px;font-size:16px;font-weight:600}h4{margin:12px 0 0;font-size:14px}
header{border-left:4px solid var(--accent);padding-left:12px}p{margin:8px 0 0}header p,.muted,footer{color:var(--muted-foreground)}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,10rem),1fr));gap:12px;margin-top:16px}
.tile{min-width:0;background:color-mix(in srgb,var(--muted) 40%,transparent);border-top:3px solid var(--accent);border-radius:8px;padding:8px 12px}
.label{font-size:11px;font-weight:500;letter-spacing:.025em;text-transform:uppercase;color:var(--muted-foreground)}
.value{font-size:18px;font-weight:600;line-height:28px;font-variant-numeric:tabular-nums;margin-top:2px;overflow-wrap:break-word}.delta{font-size:12px;font-variant-numeric:tabular-nums;margin-top:4px}
.pass,.up{color:#047857}.warn{color:#b45309}.fail,.down{color:#be123c}.dark .pass,.dark .up{color:#34d399}.dark .warn{color:#f59e0b}.dark .fail,.dark .down{color:#fb7185}
.checks-line{border:1px solid var(--border);border-radius:8px;padding:8px 12px;margin-top:16px;background:color-mix(in srgb,var(--muted) 20%,transparent)}
section{margin-top:24px}ul{padding-left:20px;margin:8px 0 0}li+li{margin-top:4px}.checks{list-style:none;padding:0}.status{font-size:11px;letter-spacing:.025em;text-transform:uppercase;margin-right:6px}
.table{margin-top:12px;overflow-x:auto;border:1px solid var(--border);border-radius:8px}.table:focus-visible{outline:2px solid var(--ring);outline-offset:2px}table{width:100%;border-collapse:collapse}th,td{padding:6px 12px;text-align:left;white-space:nowrap;border-bottom:1px solid var(--border)}thead{background:color-mix(in srgb,var(--muted) 50%,transparent)}thead th{color:var(--muted-foreground);font-size:12px;font-weight:500;border-bottom-color:var(--accent)}tbody th{font-weight:400}.numeric{text-align:right;font-variant-numeric:tabular-nums}tfoot{border-top:2px solid var(--accent);font-weight:600}
figure{margin:12px 0 0}img{display:block;width:100%;border:1px solid var(--border);border-radius:8px;background:white}footer{margin-top:24px;padding-top:12px;border-top:1px solid var(--border);font-size:12px}.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap}
@media(min-width:640px){article{padding-left:24px;padding-right:24px}}
`;

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}
function testId(element, id) {
  element.dataset.testid = id;
  return element;
}
function status(value, t) {
  const span = node("span", undefined, `status ${value}`);
  const glyph = node("span", glyphs[value]);
  glyph.setAttribute("aria-hidden", "true");
  span.append(glyph, document.createTextNode(` ${t.statuses[value]}`));
  return span;
}
function tableOf(table, title, currency) {
  const box = node("div", undefined, "table");
  box.setAttribute("role", "region");
  box.tabIndex = 0;
  const grid = node("table");
  grid.append(node("caption", title, "sr-only"));
  const head = node("thead");
  const headings = node("tr");
  table.columns.forEach((column, index) => {
    const th = node(
      "th",
      column,
      numeric.has(table.formats[index]) ? "numeric" : undefined,
    );
    th.scope = "col";
    headings.append(th);
  });
  head.append(headings);
  grid.append(head);
  const body = node("tbody");
  table.rows.forEach((row, r) => {
    const tr = node("tr");
    row.forEach((cell, c) => {
      const format = cellFormat(table, r, c);
      const td = node(
        c === 0 && format === "text" ? "th" : "td",
        formatValue(cell, format, currency),
        numeric.has(format) ? "numeric" : undefined,
      );
      if (td.tagName === "TH") td.scope = "row";
      tr.append(td);
    });
    body.append(tr);
  });
  grid.append(body);
  if (table.totals?.length) {
    const foot = node("tfoot");
    const tr = node("tr");
    table.totals.forEach((cell, c) => {
      const format = table.formats[c] ?? "text";
      tr.append(
        node(
          "td",
          formatValue(cell, format, currency),
          numeric.has(format) ? "numeric" : undefined,
        ),
      );
    });
    foot.append(tr);
    grid.append(foot);
  }
  box.append(grid);
  return box;
}

function chartFigure(chart, context, isDisposed) {
  const figure = node("figure");
  const image = node("img");
  let failed = false;
  image.alt = chart.title ?? chart.id.replaceAll("_", " ");
  image.loading = "lazy";
  image.addEventListener("error", () => {
    failed = true;
    figure.remove();
  });
  figure.append(image);
  void context
    .loadRaster(chart.png)
    .then((url) => {
      if (!isDisposed() && !failed && !context.signal.aborted) image.src = url;
    })
    .catch(() => {
      if (!isDisposed()) figure.remove();
    });
  return figure;
}

function mount(root, context) {
  const report = parseBusinessReport(context.artifact.content);
  if (!report || context.signal.aborted)
    throw new Error("Report presentation unavailable");
  const t = context.locale.startsWith("zh") ? labels.zh : labels.en;
  let disposed = false;
  const style = node("style", css);
  const card = testId(
    node("div", undefined, context.theme === "dark" ? "dark" : "light"),
    "business-report-card",
  );
  const article = node("article");
  article.style.setProperty("--accent", report.primaryColor);
  const header = node("header");
  header.append(
    node("h2", report.title),
    node(
      "p",
      [report.company, report.periodLabel, t.draft(report.draft)]
        .filter(Boolean)
        .join(" · "),
    ),
  );
  article.append(header);
  if (report.kpis.length) {
    const grid = testId(node("div", undefined, "kpis"), "business-report-kpis");
    for (const kpi of report.kpis) {
      const tile = node("div", undefined, "tile");
      tile.append(
        node("div", kpi.label, "label"),
        testId(
          node(
            "div",
            formatValue(kpi.value, kpi.format, report.currency),
            "value",
          ),
          "business-report-kpi-value",
        ),
      );
      if (kpi.delta?.pct !== undefined && kpi.delta.pct !== null) {
        const pct = kpi.delta.pct;
        const direction = pct > 0 ? "up" : pct < 0 ? "down" : "flat";
        const glyph = pct > 0 ? "▲" : pct < 0 ? "▼" : "=";
        tile.append(
          node(
            "div",
            `${glyph} ${formatValue(pct >= 0 ? pct : -pct, "percent")} ${t.compared(kpi.delta.vs)}`,
            `delta ${direction}`,
          ),
        );
      }
      grid.append(tile);
    }
    article.append(grid);
  }
  const line = checksLine(report.checks);
  if (line) {
    const worst =
      ["fail", "warn", "pass"].find((value) =>
        report.checks.some((check) => check.status === value),
      ) ?? "not_checked";
    const paragraph = testId(
      node("p", undefined, "checks-line"),
      "business-report-checks-line",
    );
    paragraph.append(status(worst, t), document.createTextNode(` ${line}`));
    article.append(paragraph);
  }
  const charts = new Map(report.charts.map((chart) => [chart.id, chart]));
  for (const section of report.sections) {
    const block = node("section");
    block.id = `report-${section.id}`;
    block.append(node("h3", section.heading));
    for (const text of section.paragraphs ?? []) block.append(node("p", text));
    if (section.bullets?.length) {
      const list = node("ul");
      section.bullets.forEach((text) => list.append(node("li", text)));
      block.append(list);
    }
    if (section.table)
      block.append(tableOf(section.table, section.heading, report.currency));
    for (const id of section.charts ?? []) {
      const chart = charts.get(id);
      if (!chart) continue;
      block.append(chartFigure(chart, context, () => disposed));
    }
    if (section.note) block.append(node("p", section.note, "muted"));
    article.append(block);
  }
  if (report.checks.length || report.notes.length) {
    const section = node("section");
    section.append(node("h3", t.checks));
    if (report.checks.length) {
      const list = node("ul", undefined, "checks");
      for (const check of report.checks) {
        const li = node("li");
        li.append(status(check.status, t), document.createTextNode(check.text));
        list.append(li);
      }
      section.append(list);
    }
    if (report.notes.length) {
      section.append(node("h4", t.omitted));
      const list = node("ul", undefined, "muted");
      report.notes.forEach((text) => list.append(node("li", text)));
      section.append(list);
    }
    article.append(section);
  }
  if (report.inputs.length)
    article.append(
      node(
        "footer",
        t.inputs(
          report.inputs.map((input) =>
            input.uploaded
              ? t.uploaded(input.name, input.uploaded)
              : input.name,
          ),
        ),
      ),
    );
  card.append(article);
  root.append(style, card);
  const basename = context.artifact.filepath
    .split("/")
    .at(-1)
    .slice(0, -".report.json".length);
  return {
    dispose() {
      disposed = true;
      root.replaceChildren();
    },
    files: {
      exports: renders.map(([kind, label]) => ({
        path: `${basename}.${kind}`,
        label,
      })),
      collection: REPORTS_FOLDER,
    },
  };
}

function fileCollection({ filepath, destination, presented }) {
  const ownPrefix = `/mnt/user-data/files/${REPORTS_FOLDER}/`;
  if (destination === "shared" && filepath.startsWith(ownPrefix)) {
    const leaf = filepath.slice(ownPrefix.length);
    return leaf && !leaf.includes("/") ? { collection: REPORTS_FOLDER } : null;
  }
  const report = reportOfRender(filepath);
  if (!report) return null;
  return presented.includes(report) ? { collection: REPORTS_FOLDER } : null;
}

export default {
  apiVersion: 1,
  module: "legacy-report.v1",
  artifactApiVersion: 1,
  artifacts: [
    { id: "report", title: "Historical report", kind: "native", mount },
  ],
  fileFilingApiVersion: 1,
  fileCollection,
};
