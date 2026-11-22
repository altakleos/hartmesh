"use client";

import { useVirtualizer } from "@tanstack/react-virtual";
import {
  useCallback,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
  type RefObject,
} from "react";

const VIRTUALIZATION_THRESHOLD = 60;

export function calculateScrollMargin(
  rootTop: number,
  scrollParentTop: number,
  scrollTop: number,
) {
  return Math.max(0, rootTop - scrollParentTop + scrollTop);
}

export function VirtualThreadList<T extends { thread_id: string }>({
  estimateSize,
  gap = 0,
  itemKey,
  items,
  precedingSection,
  renderItem,
  scrollParentSelector,
}: {
  estimateSize: number;
  gap?: number;
  itemKey?: (item: T) => string;
  items: readonly T[];
  precedingSection?: RefObject<HTMLElement | null>;
  renderItem: (thread: T, index: number) => ReactNode;
  scrollParentSelector: string;
}) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const getScrollElement = useCallback(
    () => rootRef.current?.closest<HTMLElement>(scrollParentSelector) ?? null,
    [scrollParentSelector],
  );
  const [scrollMargin, setScrollMargin] = useState(0);
  useLayoutEffect(() => {
    const root = rootRef.current;
    const scrollParent = getScrollElement();
    if (!root || !scrollParent) return;
    const measure = () =>
      setScrollMargin(
        calculateScrollMargin(
          root.getBoundingClientRect().top,
          scrollParent.getBoundingClientRect().top,
          scrollParent.scrollTop,
        ),
      );
    measure();
    const section = precedingSection?.current;
    if (items.length < VIRTUALIZATION_THRESHOLD || !section) return;
    const observer = new ResizeObserver(measure);
    observer.observe(section);
    return () => observer.disconnect();
  }, [getScrollElement, items.length, precedingSection]);
  const virtualizer = useVirtualizer({
    count: items.length,
    estimateSize: () => estimateSize,
    getItemKey: (index) => {
      const item = items[index];
      return item ? (itemKey?.(item) ?? item.thread_id) : index;
    },
    getScrollElement,
    overscan: 8,
    scrollMargin,
  });

  if (items.length < VIRTUALIZATION_THRESHOLD) {
    return (
      <div
        ref={rootRef}
        className="flex w-full flex-col"
        style={{ gap: `${gap}px` }}
      >
        {items.map(renderItem)}
      </div>
    );
  }

  return (
    <div
      ref={rootRef}
      className="relative w-full"
      style={{ height: `${virtualizer.getTotalSize()}px` }}
    >
      {virtualizer.getVirtualItems().map((virtualRow) => {
        const thread = items[virtualRow.index];
        if (!thread) return null;
        return (
          <div
            key={virtualRow.key}
            ref={virtualizer.measureElement}
            data-index={virtualRow.index}
            className="absolute top-0 left-0 w-full"
            style={{
              paddingBottom: `${gap}px`,
              transform: `translateY(${virtualRow.start - scrollMargin}px)`,
            }}
          >
            {renderItem(thread, virtualRow.index)}
          </div>
        );
      })}
    </div>
  );
}
