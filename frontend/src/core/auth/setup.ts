import { AUTH_REQUEST_TIMEOUT_MS } from "./constants";
import { parseAuthError } from "./types";

export type SetupStatusResponse = {
  needs_setup?: boolean;
  registration_enabled?: boolean;
  // Sign-on-only mode (auth.local.enabled: false on the Gateway): the
  // identity provider is the one way in, so the page shows only its sign-in.
  // Absent on older Gateways, which never close local passwords.
  sign_on_only?: boolean;
};

export type SetupStatusCheck = {
  checked: boolean;
  status: SetupStatusResponse | null;
};

export const setupStatusFetchInit = {
  cache: "no-store",
  credentials: "include",
} satisfies RequestInit;

export async function fetchSetupStatus(): Promise<SetupStatusResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), AUTH_REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch("/api/v1/auth/setup-status", {
      ...setupStatusFetchInit,
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`setup-status failed: ${response.status}`);
    }
    return (await response.json()) as SetupStatusResponse;
  } finally {
    clearTimeout(timeout);
  }
}

export function isSystemAlreadyInitializedError(data: unknown): boolean {
  return parseAuthError(data).code === "system_already_initialized";
}

export function isSignOnOnly(check: SetupStatusCheck): boolean {
  // Only a checked answer can hide the local form: while the Gateway's answer
  // is unknown the page keeps today's shape, and an older Gateway that omits
  // the field is a local-password deployment.
  return check.checked && check.status?.sign_on_only === true;
}

export function canCreateRegularAccount(check: SetupStatusCheck): boolean {
  // registration_enabled is absent on older Gateways; treat that as allowed so
  // the signup entry only disappears when the backend actively closes it.
  return (
    check.checked &&
    !isSignOnOnly(check) &&
    check.status?.needs_setup !== true &&
    check.status?.registration_enabled !== false
  );
}
