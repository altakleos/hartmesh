import { UnauthorizedError } from "@/core/api/errors";
import { userSchema } from "@/core/auth/types";
import { getSpace } from "@/core/spaces/api";

import { getInstance } from "./api";
import { hasCeiling, INSPECT } from "./presentation";

export type TransferHome = {
  accountId: string;
  instanceId: string;
  generation: number;
  homeId: string;
  homeGeneration: number;
};

export async function checkHomeTransferAccess(
  home: TransferHome,
  signal: AbortSignal,
) {
  const [response, instance, space] = await Promise.all([
    fetch("/api/v1/auth/me", {
      credentials: "include",
      cache: "no-store",
      signal,
    }),
    getInstance(home.instanceId, signal),
    getSpace(home.homeId, signal),
  ]);
  if (response.status === 401 || response.status === 403)
    throw new UnauthorizedError();
  if (!response.ok) throw new Error("Account check unavailable");
  const account = userSchema.parse(await response.json());
  signal.throwIfAborted();
  return (
    account.id === home.accountId &&
    hasCeiling(account.permissions, "agents:read") &&
    hasCeiling(account.permissions, "agents:write") &&
    instance.id === home.instanceId &&
    instance.generation === home.generation &&
    instance.home_id === home.homeId &&
    (instance.home_generation == null ||
      instance.home_generation === home.homeGeneration) &&
    instance.status === "active" &&
    (instance.permissions & INSPECT) !== 0 &&
    space.id === home.homeId &&
    space.generation === home.homeGeneration &&
    space.status === "active" &&
    (space.permissions & 3) === 3
  );
}
