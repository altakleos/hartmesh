/**
 * Download all my data: the person's own export, prepared by the Gateway.
 *
 * `POST /api/account/export` starts it (or answers with the one in
 * progress), `GET` says how it is going and lists its parts once ready,
 * `GET .../parts/{n}` is each part's download and `DELETE` throws it away.
 * Every route acts for the signed-in person only: there is no user id to
 * pass, so this page can never ask for anyone else's.
 */

import { fetch } from "../api/fetcher";
import { getBackendBaseURL } from "../config";

export type AccountExportState = "building" | "ready" | "downloaded" | "failed";

export interface AccountExportProgress {
  conversations_total: number;
  conversations_done: number;
  files_total: number;
  files_done: number;
  bytes_total: number;
  bytes_done: number;
}

export interface AccountExportPart {
  number: number;
  size: number;
  downloaded: boolean;
}

export interface AccountExportStatus {
  state: AccountExportState;
  started_at: string;
  progress: AccountExportProgress;
  parts: AccountExportPart[];
  /** How many files, conversations or documents were left out; the download's README names them. */
  skipped: number;
  /** When it is deleted if nothing more is downloaded; `null` while a part downloads. */
  expires_at: string | null;
  error?: { code: "no_space" | "failed" | string; detail: string | null };
}

/** Other people's exports are being prepared: the Gateway prepares only a few at once. */
export class AccountExportBusyError extends Error {
  constructor() {
    super("Other exports are being prepared");
    this.name = "AccountExportBusyError";
  }
}

/** This deployment cannot prepare exports (more than one Gateway process serves it). */
export class AccountExportUnavailableError extends Error {
  constructor() {
    super("Downloading all your data is not available here");
    this.name = "AccountExportUnavailableError";
  }
}

const EXPORT_PATH = "/api/account/export";

function exportURL() {
  return `${getBackendBaseURL()}${EXPORT_PATH}`;
}

/** Where part `number` downloads from: a plain link, so the browser saves it and shows its own progress. */
export function urlOfAccountExportPart(number: number): string {
  return `${exportURL()}/parts/${number}`;
}

function refuse(response: Response): never {
  if (response.status === 429) {
    throw new AccountExportBusyError();
  }
  // 403: this sign-in cannot have one (a deployment without sign-in); 503: more than one Gateway process serves it.
  if (response.status === 403 || response.status === 503) {
    throw new AccountExportUnavailableError();
  }
  throw new Error(
    `Account export request failed with status ${response.status}`,
  );
}

/** The person's export, or `null` when they have none. */
export async function getAccountExport(): Promise<AccountExportStatus | null> {
  const response = await fetch(exportURL());
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    refuse(response);
  }
  return (await response.json()) as AccountExportStatus;
}

/** Start preparing the person's export; asking again returns the one already there. */
export async function startAccountExport(): Promise<AccountExportStatus> {
  const response = await fetch(exportURL(), { method: "POST" });
  if (!response.ok) {
    refuse(response);
  }
  return (await response.json()) as AccountExportStatus;
}

/** Stop the export if it is being prepared, and delete it. */
export async function discardAccountExport(): Promise<void> {
  const response = await fetch(exportURL(), { method: "DELETE" });
  if (!response.ok && response.status !== 404) {
    refuse(response);
  }
}

/**
 * How far it got, from 0 to 100. The Gateway writes the conversations first,
 * then copies the files, so each counts for half where there are both: the
 * bar moves through both phases and never goes back.
 */
export function accountExportPercent(progress: AccountExportProgress): number {
  const fractions: number[] = [];
  if (progress.conversations_total > 0) {
    fractions.push(progress.conversations_done / progress.conversations_total);
  }
  if (progress.bytes_total > 0) {
    fractions.push(progress.bytes_done / progress.bytes_total);
  }
  const done =
    fractions.length === 0
      ? 0
      : fractions.reduce((sum, fraction) => sum + fraction, 0) /
        fractions.length;
  return Math.max(0, Math.min(100, Math.round(done * 100)));
}
