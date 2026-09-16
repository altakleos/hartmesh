"use client";

import { createContext, useContext } from "react";

/**
 * Puts the cursor back in the message box.
 *
 * Anything rendered inside the composer — the starter grid, the suggestion row
 * — can fill the box, and filling it is only half the gesture: the person
 * still has to add their file and say which month. `InputBox` owns the
 * textarea, so it supplies this; everyone else asks for it.
 *
 * The default is a no-op so a component that renders outside a composer (a
 * test, a future surface) still works rather than throwing.
 */
const noComposer = () => {
  // No composer above this component; filling a box that is not there is fine.
};

const ComposerFocusContext = createContext<() => void>(noComposer);

export const ComposerFocusProvider = ComposerFocusContext.Provider;

export function useComposerFocus(): () => void {
  return useContext(ComposerFocusContext);
}
