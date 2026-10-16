import { useEffect, useState } from "react";

import { useI18n } from "@/core/i18n/hooks";

export function ConversationFileDropHint({
  name,
  enabled,
}: {
  name: string;
  enabled: boolean;
}) {
  const { t } = useI18n();
  const [dragging, setDragging] = useState(false);
  useEffect(() => {
    if (!enabled) {
      setDragging(false);
      return;
    }
    let depth = 0;
    const enter = (event: DragEvent) => {
      if (!event.dataTransfer?.types.includes("Files")) return;
      depth += 1;
      setDragging(true);
    };
    const leave = () => {
      depth = Math.max(0, depth - 1);
      if (depth === 0) setDragging(false);
    };
    const clear = () => {
      depth = 0;
      setDragging(false);
    };
    window.addEventListener("dragenter", enter);
    window.addEventListener("dragleave", leave);
    window.addEventListener("drop", clear);
    window.addEventListener("blur", clear);
    return () => {
      window.removeEventListener("dragenter", enter);
      window.removeEventListener("dragleave", leave);
      window.removeEventListener("drop", clear);
      window.removeEventListener("blur", clear);
    };
  }, [enabled]);
  return enabled && dragging ? (
    <div
      className="bg-background/90 pointer-events-none absolute inset-0 z-50 grid place-items-center rounded-xl border-2 border-dashed p-6"
      role="status"
    >
      {t.agentInstances.files.drop(name)}
    </div>
  ) : null;
}
