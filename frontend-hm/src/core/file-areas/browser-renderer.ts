"use client";
import { useSearchParams } from "next/navigation";
import { useSyncExternalStore } from "react";

function subscribe(update: () => void) {
  const pointers = ["(any-pointer: fine)", "(any-pointer: coarse)"].map(
    (query) => window.matchMedia(query),
  );
  for (const pointer of pointers) pointer.addEventListener("change", update);
  return () => {
    for (const pointer of pointers)
      pointer.removeEventListener("change", update);
  };
}
function desktop() {
  const ua = navigator.userAgent;
  return (
    window.matchMedia("(any-pointer: fine)").matches &&
    !window.matchMedia("(any-pointer: coarse)").matches &&
    !/Android|iPhone|iPad|iPod|Mobile|Tablet/i.test(ua) &&
    !(ua.includes("Macintosh") && navigator.maxTouchPoints > 1)
  );
}
export function useBrowserRenderer(): {
  native: boolean;
  choice: "classic" | "browser" | undefined;
} {
  const choice = useSearchParams()?.get("view");
  const qualifiedDesktop = useSyncExternalStore(
    subscribe,
    desktop,
    () => false,
  );
  return {
    native: choice === "browser" || (choice !== "classic" && qualifiedDesktop),
    choice: choice === "classic" || choice === "browser" ? choice : undefined,
  };
}
