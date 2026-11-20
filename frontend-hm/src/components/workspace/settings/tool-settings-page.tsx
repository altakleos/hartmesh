"use client";

import { PencilIcon, Trash2 } from "lucide-react";
import { useLayoutEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "@/components/ui/item";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { useAuth } from "@/core/auth/AuthProvider";
import { useCustomerAdministration } from "@/core/features/hooks";
import { useI18n } from "@/core/i18n/hooks";
import {
  checkMCPConnection,
  MCPConfigRequestError,
  type ConnectionCheck,
} from "@/core/mcp/api";
import { formCanEdit } from "@/core/mcp/connection-form";
import {
  useEnableMCPServer,
  useMCPConfig,
  useMCPServerMutation,
} from "@/core/mcp/hooks";
import {
  formatMCPServerDefinition,
  MCPServerDefinitionError,
  parseMCPServerDefinition,
} from "@/core/mcp/parse";
import type { MCPServerConfig } from "@/core/mcp/types";
import { env } from "@/env";

import { ConnectionForm } from "./connection-form";
import { ProviderEnablementNotice } from "./provider-enablement-notice";
import { SettingsSection } from "./settings-section";

function isLocalDefinition(server: MCPServerConfig) {
  const transport = server.type ?? server.transport;
  return !transport || transport === "stdio";
}

export function ToolSettingsPage() {
  const { t } = useI18n();
  const { user } = useAuth();
  const { config, isLoading, error } = useMCPConfig();
  const adminRequired =
    error instanceof MCPConfigRequestError && error.isAdminRequired;
  return (
    <SettingsSection
      title={t.settings.tools.title}
      description={t.settings.tools.description}
    >
      <div className="flex w-full flex-col gap-4">
        {isLoading ? (
          <div className="text-muted-foreground text-sm">
            {t.common.loading}
          </div>
        ) : adminRequired ? (
          <div className="text-muted-foreground text-sm">
            {t.settings.tools.adminRequired}
          </div>
        ) : error ? (
          <div>Error: {error.message}</div>
        ) : (
          config && (
            <MCPServerList
              key={JSON.stringify([user?.id, user?.system_role])}
              servers={config.mcp_servers}
            />
          )
        )}
      </div>
    </SettingsSection>
  );
}

function MCPServerList({
  servers,
}: {
  servers?: Record<string, MCPServerConfig>;
}) {
  const { t } = useI18n();
  const { isPending, mutate: enableMCPServer } = useEnableMCPServer();
  const { isPending: isWriting, mutate: mutateServer } = useMCPServerMutation();
  const [editor, setEditor] = useState<
    | { mode: "add" }
    | { mode: "edit"; name: string; original: MCPServerConfig }
    | null
  >(null);
  const [editorKind, setEditorKind] = useState<"form" | "json">("form");
  const [check, setCheck] = useState<{
    name: string;
    saved: boolean;
    definition: string;
    result: ConnectionCheck | "pending" | "unavailable";
  } | null>(null);
  const checker = useRef<{
    controller: AbortController;
    name: string;
    definition: string;
    prior?: string;
    observed: boolean;
  } | null>(null);
  const listed = useRef(servers);
  useLayoutEffect(() => {
    listed.current = servers;
    const owner = checker.current;
    if (!owner) return;
    const current = JSON.stringify(servers?.[owner.name]);
    if (current === owner.definition) owner.observed = true;
    // A just-saved list may still show the prior row until its refetch completes.
    else if (owner.observed || owner.prior !== current) {
      owner.controller.abort();
      checker.current = null;
      setCheck(null);
    }
  }, [servers]);
  const mounted = useRef(false);
  useLayoutEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      checker.current?.controller.abort();
    };
  }, []);
  function clearCheck() {
    checker.current?.controller.abort();
    checker.current = null;
    setCheck(null);
  }
  async function runCheck(
    name: string,
    saved: boolean,
    server: MCPServerConfig,
  ) {
    if (!mounted.current) return;
    checker.current?.controller.abort();
    const owner = new AbortController();
    const definition = JSON.stringify(server);
    const prior = JSON.stringify(listed.current?.[name]);
    checker.current = {
      controller: owner,
      name,
      definition,
      prior,
      observed: prior === definition,
    };
    setCheck({ name, saved, definition, result: "pending" });
    try {
      const result = await checkMCPConnection(name, owner.signal);
      if (!owner.signal.aborted) setCheck({ name, saved, definition, result });
    } catch {
      if (!owner.signal.aborted)
        setCheck({ name, saved, definition, result: "unavailable" });
    }
  }
  const [definition, setDefinition] = useState("");
  const [definitionError, setDefinitionError] = useState<string | null>(null);
  const [pendingRemoval, setPendingRemoval] = useState<string | null>(null);

  const readOnly = env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY === "true";
  const { localMcpManagement } = useCustomerAdministration();
  const current = servers ?? {};
  const editorStale =
    editor?.mode === "edit" &&
    JSON.stringify(editor.original) !== JSON.stringify(current[editor.name]);
  const entries = Object.entries(current);
  const isMutating = isPending || isWriting;

  function displayServerName(name: string | null) {
    return name === null || name.length === 0
      ? t.settings.tools.unnamedServer
      : name;
  }

  function closeEditor() {
    setEditor(null);
    setDefinition("");
    setDefinitionError(null);
  }

  function openAddEditor() {
    clearCheck();
    setEditorKind("form");
    setDefinition("");
    setDefinitionError(null);
    setEditor({ mode: "add" });
  }

  function openEditEditor(name: string, config: MCPServerConfig) {
    clearCheck();
    setEditorKind(formCanEdit(config) ? "form" : "json");
    setDefinition(formatMCPServerDefinition(name, config));
    setDefinitionError(null);
    setEditor({ mode: "edit", name, original: config });
  }

  function handleSaveDefinition() {
    if (editor === null) {
      return;
    }

    let parsed: Record<string, MCPServerConfig>;
    try {
      parsed = parseMCPServerDefinition(definition);
    } catch (parseError) {
      if (parseError instanceof MCPServerDefinitionError) {
        const messages = {
          emptyDefinition: t.settings.tools.definitionEmpty,
          invalidJson: t.settings.tools.definitionInvalidJson,
          rootNotObject: t.settings.tools.definitionRootNotObject,
          emptyServerMap: t.settings.tools.definitionNoServers,
          serverConfigNotObject:
            t.settings.tools.definitionServerNotObject.replace(
              "{name}",
              parseError.serverName ?? "",
            ),
        };
        setDefinitionError(messages[parseError.code]);
      } else {
        setDefinitionError(t.settings.tools.definitionInvalidJson);
      }
      return;
    }

    if (!localMcpManagement && Object.values(parsed).some(isLocalDefinition)) {
      setDefinitionError(t.settings.providerEnablement);
      return;
    }

    if (editor.mode === "add") {
      const duplicate = Object.keys(parsed).find((name) =>
        Object.hasOwn(current, name),
      );
      if (duplicate !== undefined) {
        setDefinitionError(
          t.settings.tools.serverAlreadyExists.replace("{name}", duplicate),
        );
        return;
      }
      setDefinitionError(null);
      mutateServer(
        { operation: "create", servers: parsed },
        { onSuccess: closeEditor },
      );
    } else {
      const editedEntries = Object.entries(parsed);
      if (editedEntries.length !== 1) {
        setDefinitionError(t.settings.tools.editSingleServer);
        return;
      }
      const [editedName, editedConfig] = editedEntries[0]!;
      if (editedName !== editor.name) {
        setDefinitionError(
          t.settings.tools.editServerNameMismatch.replace(
            "{name}",
            editor.name,
          ),
        );
        return;
      }
      setDefinitionError(null);
      mutateServer(
        {
          operation: "update",
          serverName: editor.name,
          server: editedConfig,
        },
        { onSuccess: closeEditor },
      );
    }
  }

  function handleRemove(name: string) {
    mutateServer(
      { operation: "delete", serverName: name },
      { onSuccess: () => setPendingRemoval(null) },
    );
  }

  return (
    <div className="flex w-full flex-col gap-4">
      {!localMcpManagement && <ProviderEnablementNotice />}
      <div className="flex justify-end">
        <Button
          size="sm"
          variant="outline"
          disabled={readOnly || isMutating}
          onClick={openAddEditor}
        >
          {t.settings.tools.connection.add}
        </Button>
      </div>

      {entries.length === 0 ? (
        <div className="text-muted-foreground text-sm">
          {t.settings.tools.empty}
        </div>
      ) : (
        entries.map(([name, config]) => {
          const displayName = displayServerName(name);
          const currentCheck =
            check?.name === name && check.definition === JSON.stringify(config)
              ? check
              : null;
          const localReadOnly =
            isLocalDefinition(config) && !localMcpManagement;
          return (
            <Item className="w-full" variant="outline" key={name}>
              <ItemContent>
                <ItemTitle>
                  <div className="flex items-center gap-2">
                    <div>{displayName}</div>
                  </div>
                </ItemTitle>
                <ItemDescription className="line-clamp-4">
                  {config.description}
                </ItemDescription>
                {currentCheck && (
                  <p role="status" className="text-sm">
                    {currentCheck.saved &&
                      t.settings.tools.connection.saved + " "}
                    {currentCheck.result === "pending"
                      ? t.settings.tools.connection.checking
                      : currentCheck.result === "unavailable"
                        ? t.settings.tools.connection.unavailable
                        : currentCheck.result.status === "connected"
                          ? t.settings.tools.connection.connected(
                              currentCheck.result.tool_count,
                            )
                          : t.settings.tools.connection.reasons[
                              currentCheck.result.reason
                            ]}
                  </p>
                )}
              </ItemContent>
              <ItemActions className="gap-1">
                {!isLocalDefinition(config) && (
                  <Button
                    aria-label={`${t.settings.tools.connection.check} ${displayName}`}
                    size="sm"
                    variant="outline"
                    disabled={
                      readOnly ||
                      isMutating ||
                      currentCheck?.result === "pending"
                    }
                    onClick={() => void runCheck(name, false, config)}
                  >
                    {t.settings.tools.connection.check}
                  </Button>
                )}
                <Switch
                  checked={config.enabled}
                  disabled={readOnly || localReadOnly || isMutating}
                  onCheckedChange={(checked) => {
                    clearCheck();
                    enableMCPServer({ serverName: name, enabled: checked });
                  }}
                />
                <Button
                  size="icon"
                  variant="ghost"
                  aria-label={`${t.common.edit} ${displayName}`}
                  disabled={readOnly || localReadOnly || isMutating}
                  onClick={() => openEditEditor(name, config)}
                >
                  <PencilIcon className="size-4" />
                </Button>
                <Button
                  size="icon"
                  variant="ghost"
                  aria-label={`${t.common.delete} ${displayName}`}
                  disabled={readOnly || localReadOnly || isMutating}
                  onClick={() => {
                    clearCheck();
                    setPendingRemoval(name);
                  }}
                >
                  <Trash2 className="size-4" />
                </Button>
              </ItemActions>
            </Item>
          );
        })
      )}

      <Dialog
        open={editor !== null}
        onOpenChange={(open) => !open && !isWriting && closeEditor()}
      >
        <DialogContent className="max-h-[85dvh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editorKind === "form"
                ? editor?.mode === "edit"
                  ? t.settings.tools.connection.edit
                  : t.settings.tools.connection.add
                : editor?.mode === "edit"
                  ? t.settings.tools.editServer
                  : t.settings.tools.addServer}
            </DialogTitle>
            <DialogDescription>
              {editorKind === "form"
                ? t.settings.tools.connection.formDescription
                : editor?.mode === "edit"
                  ? t.settings.tools.editServerDescription.replace(
                      "{name}",
                      editor.name,
                    )
                  : t.settings.tools.addServerDescription}
            </DialogDescription>
          </DialogHeader>
          {editorStale ? (
            <>
              <p role="alert">{t.settings.tools.connection.changed}</p>
              <DialogFooter>
                <Button variant="outline" onClick={closeEditor}>
                  {t.common.cancel}
                </Button>
              </DialogFooter>
            </>
          ) : editorKind === "form" && editor ? (
            <ConnectionForm
              name={editor.mode === "edit" ? editor.name : undefined}
              original={editor.mode === "edit" ? editor.original : undefined}
              servers={current}
              pending={isWriting}
              onCancel={closeEditor}
              onJson={() => setEditorKind("json")}
              onSave={(name, server) => {
                mutateServer(
                  editor.mode === "edit"
                    ? { operation: "update", serverName: name, server }
                    : { operation: "create", servers: { [name]: server } },
                  {
                    onSuccess: (saved) => {
                      closeEditor();
                      const server = saved.mcp_servers[name];
                      if (server) void runCheck(name, true, server);
                    },
                  },
                );
              }}
            />
          ) : (
            <>
              <Textarea
                className="min-h-52 font-mono text-xs"
                aria-label={t.settings.tools.serverDefinitionLabel}
                spellCheck={false}
                value={definition}
                placeholder={
                  localMcpManagement
                    ? t.settings.tools.addServerPlaceholder
                    : JSON.stringify(
                        {
                          mcpServers: {
                            "my-service": {
                              type: "http",
                              url: "https://mcp.example.com/mcp",
                              headers: { Authorization: "Bearer <token>" },
                            },
                          },
                        },
                        null,
                        2,
                      )
                }
                onChange={(event) => setDefinition(event.target.value)}
              />
              {definitionError && (
                <div className="text-destructive text-sm" role="alert">
                  {definitionError}
                </div>
              )}
              <DialogFooter>
                <Button
                  variant="outline"
                  disabled={isWriting}
                  onClick={closeEditor}
                >
                  {t.common.cancel}
                </Button>
                <Button disabled={isWriting} onClick={handleSaveDefinition}>
                  {isWriting ? t.common.loading : t.common.save}
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>

      <Dialog
        open={pendingRemoval !== null}
        onOpenChange={(open) => !open && setPendingRemoval(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t.settings.tools.removeServer}</DialogTitle>
            <DialogDescription>
              {t.settings.tools.removeServerDescription.replace(
                "{name}",
                displayServerName(pendingRemoval),
              )}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={isWriting}
              onClick={() => setPendingRemoval(null)}
            >
              {t.common.cancel}
            </Button>
            <Button
              variant="destructive"
              disabled={isWriting}
              onClick={() =>
                pendingRemoval !== null && handleRemove(pendingRemoval)
              }
            >
              {isWriting ? t.common.loading : t.common.delete}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
