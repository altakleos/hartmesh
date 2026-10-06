import type { Message } from "@langchain/langgraph-sdk";

import type { ArtifactViewDocument } from "@/core/artifact-views/contract";
import type { Translations } from "@/core/i18n";
import type { AgentThread } from "@/core/threads/types";

export type ExtensionSettings = Readonly<
  Record<string, boolean | number | string>
>;
export type SurfaceSlot = "page";
export type SurfaceContext = {
  namespace: string;
  locale: string;
  settings: ExtensionSettings;
  threadId?: string;
  signal: AbortSignal;
  callBackend: FrontendServices["callBackend"];
  /** Host resolves current thread ownership before navigating; unavailable on older hosts. */
  openConversation?: (threadId: string) => Promise<void>;
};
export type PluginSurface = {
  id: string;
  slot: SurfaceSlot;
  title: string;
  /** Optional sidebar entry for a page surface; the host owns its URL. */
  navigation?: { label: string; labelZh?: string; icon?: string };
  /** Mount synchronously; async work must observe context.signal. */
  mount: (
    root: HTMLElement,
    context: SurfaceContext,
  ) => { dispose: () => void };
};
export type FrontendContribution = {
  viewer_id?: string | null;
  namespace: string;
  module: string | null;
  entry: string | null;
  /** Known transports: inline-v1 and assets-v1; unknown values fail per plugin. */
  transport?: string | null;
  title: string;
  description: string;
  settings: ExtensionSettings;
  backend_actions?: string[];
  artifact_presentations?: ArtifactPresentationDescriptor[];
};
export type ArtifactPresentationDescriptor = {
  id: string;
  suffixes: string[];
  source_max_bytes: number;
  preview_max_bytes: number;
  projection_marker: string | null;
};
export type ArtifactSurfaceContext = SurfaceContext & {
  artifact: Readonly<{
    filepath: string;
    threadId: string;
    content: string;
    revision: string;
    projected: boolean;
    presented: readonly string[];
  }>;
  theme: "light" | "dark";
  loadRaster: (relative: string) => Promise<string>;
};
export type PluginArtifactSurface = {
  id: string;
  title: string;
} & (
  | {
      kind: "native";
      mount: (
        root: HTMLElement,
        context: ArtifactSurfaceContext,
      ) => {
        dispose: () => void;
        files?: {
          exports: { path: string; label: string }[];
          collection?: string;
        };
      };
    }
  | {
      kind: "passive";
      present: (
        context: ArtifactSurfaceContext,
      ) => ArtifactViewDocument | Promise<ArtifactViewDocument>;
    }
);
export type FileCollectionContext = Readonly<{
  filepath: string;
  destination: "my-files" | "shared";
  presented: readonly string[];
}>;
export type ConversationActionContext = {
  thread: AgentThread;
  messages?: Message[];
  signal?: AbortSignal;
};
export type FrontendServices = {
  callBackend: (
    action: string,
    payload: Record<string, unknown>,
  ) => Promise<unknown>;
  conversationText: (context: ConversationActionContext) => Promise<string>;
  latestVisibleAnswer?: (
    context: ConversationActionContext,
  ) => Promise<{ id: string; text: string } | null>;
  showMessage: (message: string) => void;
};
export type ConversationAction = {
  id: string;
  label: string;
  icon: string;
  available: (settings: ExtensionSettings) => boolean;
  execute: (
    context: ConversationActionContext,
    services: FrontendServices,
  ) => Promise<void>;
};
export type ConversationActionGroup = {
  label: string;
  icon: string;
  actions: ConversationAction[];
};

/** Browser package API v1. Modules are installed by trusted deployment operators. */
export interface FrontendExtension {
  apiVersion: 1;
  module: string;
  icon?: string;
  surfaces?: PluginSurface[];
  /** Additive capability; the page-only v1 array stays unchanged. */
  artifactApiVersion?: 1;
  artifacts?: PluginArtifactSurface[];
  /** Optional filing policy from installed code; passive files cannot register it. */
  fileFilingApiVersion?: 1;
  fileCollection?: (
    context: FileCollectionContext,
  ) => { collection: string } | null;
  conversationActions?: (
    t: Translations,
    locale?: string,
  ) => ConversationActionGroup;
}
