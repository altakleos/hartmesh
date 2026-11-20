import { useMemo, useState } from "react";

import type { SavedAttachment } from "./conversation-file-import";

interface ImportChip {
  file: SavedAttachment;
  sourceTitle: string;
}
export function useComposerImports(scope: string) {
  const owner = useMemo(() => ({ scope }), [scope]);
  const [state, setState] = useState({ owner, chips: [] as ImportChip[] });
  if (owner !== state.owner) setState({ owner, chips: [] });
  const chips = owner === state.owner ? state.chips : [];
  function remove(captured: ImportChip[]) {
    setState((prev) =>
      prev.owner !== owner
        ? prev
        : {
            owner,
            chips: prev.chips.filter((chip) => !captured.includes(chip)),
          },
    );
  }
  function add(file: SavedAttachment, sourceTitle: string) {
    setState((prev) =>
      prev.owner !== owner ||
      prev.chips.some((chip) => chip.file.path === file.path)
        ? prev
        : { owner, chips: [...prev.chips, { file, sourceTitle }] },
    );
  }
  return { owner, chips, add, remove };
}
