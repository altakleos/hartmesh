import { expect, test } from "@rstest/core";

import { InstanceApiError } from "@/core/agent-instances/api";
import {
  readRefusal,
  refusalSentence,
  rememberRefusal,
  takeRefusal,
} from "@/core/agent-instances/errors";

test("reads an HTTP refusal without interpreting the JavaScript error name as a frame", () => {
  const body = {
    detail: "diagnostic only",
    reason: "execution-unavailable",
    execution_reason: "operator-hold",
    cause: "probe",
    allowed: 0,
  };
  expect(readRefusal(InstanceApiError.fromResponse(409, body))).toEqual({
    status: 409,
    reason: "execution-unavailable",
    executionReason: "operator-hold",
    detail: "diagnostic only",
    extra: { detail: "diagnostic only", cause: "probe", allowed: 0 },
  });
  expect(readRefusal(new InstanceApiError(404, "missing"))).toBeNull();
  expect(readRefusal(body)).toEqual({
    reason: "execution-unavailable",
    executionReason: "operator-hold",
    detail: "diagnostic only",
    extra: { detail: "diagnostic only", cause: "probe", allowed: 0 },
  });
});

test("reads the SDK's exact HTTP prefix and retains the HTTP status", () => {
  const error = Object.assign(
    new Error(
      'HTTP 409: {"detail":"private diagnostic","reason":"files-full","mine":2}',
    ),
    { status: 409 },
  );
  expect(readRefusal(error)).toEqual({
    status: 409,
    reason: "files-full",
    detail: "private diagnostic",
    extra: { detail: "private diagnostic", mine: 2 },
  });
});

test.each([
  null,
  undefined,
  [],
  "busy",
  {},
  new Error("connection"),
  { name: "TypeError", message: "connection" },
  { status: 409, message: 'HTTP 503: {"reason":"busy"}' },
  { status: 409, message: "HTTP 409: invalid" },
  { reason: "" },
  { reason: 3 },
])("ignores a source without a structured refusal: %j", (source) => {
  // A plain frame may have any name; a JavaScript error must not be mistaken for one.
  if (
    source &&
    typeof source === "object" &&
    !(source instanceof Error) &&
    "name" in source
  ) {
    expect(refusalSentence(readRefusal(source)!, "conversation")).toBeNull();
  } else expect(readRefusal(source)).toBeNull();
});

test("reads raw frame data before the SDK can discard its reason", () => {
  expect(
    readRefusal({
      name: "ResidentInterrupted",
      message: "diagnostic",
      reason: "storage-recovery",
    }),
  ).toEqual({
    name: "ResidentInterrupted",
    reason: "storage-recovery",
    detail: "diagnostic",
    extra: {},
  });
});

test("keeps the latest refusal separately for each thread and consumes it once", () => {
  const first = { reason: "busy", extra: {} };
  const latest = { reason: "files-full", extra: {} };
  rememberRefusal("refusal-a", first);
  rememberRefusal("refusal-b", first);
  rememberRefusal("refusal-a", latest);
  expect(takeRefusal("refusal-a")).toBe(latest);
  expect(takeRefusal("refusal-a")).toBeNull();
  expect(takeRefusal("refusal-b")).toBe(first);
});

test.each([
  ["busy", "agentInstances.busy", "agentWork.attempt.busy"],
  [
    "run-limit",
    "agentInstances.conversationLimit",
    "agentWork.attempt.runLimit",
  ],
  ["restarting", "agentInstances.restarting", "agentWork.attempt.restarting"],
  [
    "starting-elsewhere",
    "agentInstances.startingElsewhere",
    "agentWork.attempt.startingElsewhere",
  ],
  [
    "start-failed",
    "runProgress.residentStartFailed",
    "agentWork.attempt.startFailed",
  ],
  ["files-full", "agentInstances.fullStart", "agentWork.attempt.full"],
] as const)(
  "maps %s for a conversation and assignment",
  (reason, conversation, work) => {
    expect(refusalSentence({ reason, extra: {} }, "conversation")).toBe(
      conversation,
    );
    expect(refusalSentence({ reason, extra: {} }, "work")).toBe(work);
  },
);

test.each([
  ["storage-busy", "agentInstances.filesBusy"],
  ["file-too-large", "agentInstances.files.tooLarge"],
  ["file-write-failed", "agentInstances.files.refused"],
  ["invalid-name", "agentInstances.files.invalidName"],
  ["reserved-path", "agentInstances.files.reserved"],
  ["name-collision", "agentInstances.files.nameTaken"],
  ["file-changed", "agentInstances.files.fileChanged"],
  ["directory-not-empty", "agentInstances.files.folderNotEmpty"],
  ["agent-not-active", "agentInstances.files.notActive"],
  ["not-found", "agentInstances.files.notFound"],
  ["not-removable", "agentInstances.unfinishedCompleted"],
  ["ai-assistant-none-free", "agentInstances.assistants.noneFree"],
  ["no-running-attempt", "agentWork.stopNothing"],
  ["owned-space", "storageSpaces.agentFilesOf"],
  ["managers-only", "sandboxNetwork.managersOnly"],
  ["new-server-reason", "agentInstances.operationError"],
] as const)(
  "maps the structured reason %s without server prose",
  (reason, key) => {
    expect(
      refusalSentence({ reason, detail: "never display", extra: {} }, "files"),
    ).toBe(key);
  },
);

test("uses agent file wording for uploads, distinct from run admission", () => {
  expect(refusalSentence({ reason: "files-full", extra: {} }, "files")).toBe(
    "agentInstances.files.full",
  );
});

test.each([
  "ResidentBusy",
  "ResidentRunLimit",
  "ResidentRestarting",
  "ResidentStartingElsewhere",
  "ResidentStartFailed",
  "ResidentFilesFull",
  "ResidentFilesBusy",
])("recognizes a known frame even when its reason was lost: %s", (name) => {
  expect(refusalSentence({ name, extra: {} }, "conversation")).not.toBeNull();
});

test.each(["start-failed", "container-absent", "anything", undefined])(
  "keeps interrupted frame precedence for %s",
  (reason) => {
    expect(
      refusalSentence(
        { name: "ResidentInterrupted", reason, extra: {} },
        "conversation",
      ),
    ).toBe("agentInstances.interrupted");
  },
);
test.each([
  ["lifecycle", "interruptedByManager"],
  ["storage-recovery", "interruptedByRecovery"],
])("explains an interruption caused by %s", (reason, key) => {
  expect(
    refusalSentence(
      { name: "ResidentInterrupted", reason, extra: {} },
      "conversation",
    ),
  ).toBe(`agentInstances.${key}`);
});

test.each(["AgentDenied", "BackingUnavailable"])(
  "uses interruption as the fallback for %s",
  (name) => {
    expect(refusalSentence({ name, extra: {} }, "conversation")).toBe(
      "agentInstances.interrupted",
    );
    expect(
      refusalSentence(
        { name, reason: "files-full", extra: {} },
        "conversation",
      ),
    ).toBe("agentInstances.fullStart");
  },
);

test("leaves unknown frame errors to the existing error handler", () => {
  expect(
    refusalSentence({ name: "FutureFrame", extra: {} }, "conversation"),
  ).toBeNull();
});

test.each(["toString", "constructor", "__proto__"])(
  "treats prototype property %s as an unknown transport code",
  (value) => {
    expect(refusalSentence({ reason: value, extra: {} }, "conversation")).toBe(
      "agentInstances.operationError",
    );
    expect(
      refusalSentence({ name: value, extra: {} }, "conversation"),
    ).toBeNull();
    expect(
      refusalSentence(
        { reason: "execution-unavailable", executionReason: value, extra: {} },
        "conversation",
      ),
    ).toBe("sidebar.agentsUnavailable");
  },
);

test.each([
  ["operator-hold", "agentInstances.unavailable.agentsPaused"],
  ["host-replaced", "agentInstances.unavailable.serverMoved"],
  ["space-unverified", "agentInstances.unavailable.filesUnchecked"],
  ["storage-unavailable", "sidebar.agentsUnavailable"],
  ["agents-none", "sidebar.agentsUnavailable"],
  ["space-not-assigned", "sidebar.agentsUnavailable"],
  ["unknown", "sidebar.agentsUnavailable"],
])(
  "maps execution reason %s with context deferred to the caller",
  (executionReason, key) => {
    expect(
      refusalSentence(
        { reason: "execution-unavailable", executionReason, extra: {} },
        "conversation",
      ),
    ).toBe(key);
  },
);

test.each([
  ["ai-assistant-limit", { per_user: 0 }, "assistants.notOffered"],
  ["ai-assistant-limit", { per_user: 2 }, "assistants.limitReached"],
  ["ai-employee-limit", { allowed: 0 }, "employees.notOffered"],
  ["ai-employee-limit", { allowed: 2 }, "employees.limitReached"],
])("uses refusal counts for %s", (reason, extra, key) => {
  expect(refusalSentence({ reason, extra }, "conversation")).toBe(
    `agentInstances.${key}`,
  );
});
