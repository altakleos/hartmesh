// Rebuild portable modules with the already available compiler. No installs.
import { createRequire } from "node:module";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const require = createRequire(
  resolve(root, "../../../../frontend-hm/package.json"),
);
const ts = require("typescript");
for (const name of ["report", "format", "paths"]) {
  const source = readFileSync(resolve(root, `browser/${name}.ts`), "utf-8");
  const output = ts
    .transpileModule(source, {
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
      },
    })
    .outputText.replaceAll('"./format"', '"./format.mjs"');
  writeFileSync(
    resolve(root, `hartmesh_legacy_report/static/${name}.mjs`),
    output,
    "utf-8",
  );
}
