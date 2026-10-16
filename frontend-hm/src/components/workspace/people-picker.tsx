"use client";

import { useCallback, useEffect, useId, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/core/auth/AuthProvider";
import { useFeatures } from "@/core/features";
import { selectPeopleSearch } from "@/core/features/api";
import { useI18n } from "@/core/i18n/hooks";
import { searchPeople, type Person } from "@/core/people/api";
import { isIMEComposing } from "@/lib/ime";

interface PeoplePickerProps {
  onChange: (person: Person) => void;
  value?: Person | null;
  includeSelf?: boolean;
  existingIds?: readonly string[];
  label?: string;
  disabled?: boolean;
}

/** The draft and every search belong to one mounted account/authority scope. */
export function PeoplePicker(props: PeoplePickerProps) {
  const { user } = useAuth();
  return (
    <Picker
      key={`${user?.id}:${user?.system_role}:${user?.permissions?.join(",")}`}
      {...props}
    />
  );
}

function Picker({
  onChange,
  value,
  includeSelf = false,
  existingIds = [],
  label,
  disabled = false,
}: PeoplePickerProps) {
  const { user } = useAuth();
  const { t } = useI18n();
  const copy = t.agentInstances.access;
  const discovery = useFeatures(selectPeopleSearch);
  const mode =
    !discovery.isError && discovery.data === "prefix" ? "prefix" : "exact";
  const id = useId();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(-1);
  const [answer, setAnswer] = useState<{
    scope: string;
    users: Person[];
    state: "loading" | "ready" | "error";
  }>();
  const current = useRef<AbortController | null>(null);
  const sequence = useRef(0);
  const text = query.trim();
  const length = Array.from(text).length;
  const valid = length >= 2 && length <= (mode === "prefix" ? 64 : 320);
  const scope = `${mode}\0${query}`;
  const result = answer?.scope === scope && !disabled ? answer : undefined;
  const busy = result?.state === "loading";

  const cancel = useCallback(() => {
    sequence.current += 1;
    current.current?.abort();
    current.current = null;
  }, []);

  const find = useCallback(async () => {
    if (!valid || disabled) return;
    cancel();
    const controller = new AbortController();
    current.current = controller;
    const captured = sequence.current;
    setActive(-1);
    setAnswer({ scope, state: "loading", users: [] });
    try {
      const { users } = await searchPeople(text, controller.signal, 20);
      if (!controller.signal.aborted && sequence.current === captured)
        setAnswer({ scope, state: "ready", users });
    } catch {
      if (!controller.signal.aborted && sequence.current === captured)
        setAnswer({ scope, state: "error", users: [] });
    }
  }, [valid, disabled, cancel, scope, text]);

  useEffect(() => {
    cancel();
    setActive(-1);
    const timer =
      mode === "prefix" && valid && !disabled
        ? setTimeout(() => {
            void find();
          }, 300)
        : undefined;
    return () => {
      clearTimeout(timer);
      cancel();
    };
  }, [mode, valid, disabled, find, cancel]);

  const self: Person | null =
    includeSelf && user
      ? { id: user.id, email: user.email, name: null, active: true }
      : null;
  const options = [
    ...(self ? [self] : []),
    ...(result?.users ?? []).filter(
      (person) => person.active && person.id !== user?.id,
    ),
  ];
  const enabledOptions = options
    .map((person, index) => (existingIds.includes(person.id) ? -1 : index))
    .filter((index) => index >= 0);
  function select(index: number) {
    const person = options[index];
    if (person && !disabled && !existingIds.includes(person.id))
      onChange(person);
  }

  return (
    <div className="space-y-2">
      <label htmlFor={id}>{label ?? copy.add}</label>
      <p id={`${id}-hint`} className="text-muted-foreground text-sm">
        {mode === "prefix" ? copy.searchHint : copy.searchHintFull}
      </p>
      <div className="flex gap-2">
        <Input
          id={id}
          role="combobox"
          aria-autocomplete="list"
          aria-controls={`${id}-results`}
          aria-expanded={options.length > 0}
          aria-activedescendant={
            active >= 0 && options[active]
              ? `${id}-option-${active}`
              : undefined
          }
          aria-describedby={`${id}-hint`}
          value={query}
          disabled={disabled}
          maxLength={mode === "prefix" ? 64 : 320}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (isIMEComposing(event)) return;
            if (event.key === "ArrowDown" || event.key === "ArrowUp") {
              event.preventDefault();
              const index = enabledOptions.indexOf(active);
              const next =
                event.key === "ArrowDown"
                  ? index + 1
                  : index < 0
                    ? enabledOptions.length - 1
                    : index - 1;
              setActive(
                enabledOptions[
                  (next + enabledOptions.length) % enabledOptions.length
                ] ?? -1,
              );
            } else if (event.key === "Enter") {
              event.preventDefault();
              event.stopPropagation();
              if (active >= 0) select(active);
              else if (mode === "exact" && !busy) void find();
            }
          }}
        />
        {mode === "exact" && (
          <Button
            type="button"
            variant="outline"
            disabled={disabled || !valid || busy}
            onClick={() => void find()}
          >
            {copy.find}
          </Button>
        )}
      </div>
      {busy && <p role="status">{t.common.loading}</p>}
      {result?.state === "error" && <p role="alert">{copy.searchFailed}</p>}
      {result?.state === "ready" &&
        result.users.filter((person) => person.id !== user?.id && person.active)
          .length === 0 && (
          <p role="status">
            {mode === "prefix" ? copy.noResults : copy.notFoundFull}
          </p>
        )}
      <div
        id={`${id}-results`}
        role="listbox"
        aria-label={label ?? copy.add}
        className="space-y-1"
      >
        {options.map((person, index) => {
          const trimmed = person.name?.trim();
          const name = trimmed?.length ? trimmed : person.email;
          const already = existingIds.includes(person.id);
          const isSelf = person.id === user?.id;
          return (
            <button
              type="button"
              id={`${id}-option-${index}`}
              key={person.id}
              role="option"
              aria-selected={
                active >= 0 ? active === index : value?.id === person.id
              }
              aria-disabled={already || disabled}
              disabled={already || disabled}
              tabIndex={-1}
              className="hover:bg-muted aria-selected:bg-muted flex w-full flex-col rounded border p-2 text-left disabled:opacity-50"
              onClick={() => select(index)}
            >
              <span>{isSelf ? t.people.you : name}</span>
              {!isSelf && trimmed?.length && (
                <span className="text-muted-foreground text-sm">
                  {person.email}
                </span>
              )}
              {already && (
                <span className="text-muted-foreground text-sm">
                  {copy.alreadyHasAccess(name)}
                </span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}
