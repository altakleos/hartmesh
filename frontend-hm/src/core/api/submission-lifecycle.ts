/** Private object identity only: no UI admission tokens enter wire metadata. */
export interface SubmissionOrigin {
  readonly sequence: number;
  readonly threadId: string;
  retired: boolean;
}
interface SubmissionObserver {
  origin: SubmissionOrigin;
  created: (meta: { thread_id: string; run_id: string }) => void;
  failed: (error: unknown) => void;
}
const submissions = new WeakMap<object, SubmissionObserver>();
const failures = new WeakMap<object, SubmissionOrigin>();
function object(value: unknown): value is object {
  return typeof value === "object" && value !== null;
}
export function observeSubmission(
  input: object,
  metadata: object,
  observer: SubmissionObserver,
) {
  submissions.set(input, observer);
  submissions.set(metadata, observer);
}
export function submissionErrorOrigin(error: unknown) {
  return object(error) ? failures.get(error) : undefined;
}
export function submissionCreated(
  key: unknown,
  meta: { thread_id: string; run_id: string },
) {
  if (object(key)) submissions.get(key)?.created(meta);
}
export function submissionFailed(key: unknown, error: unknown) {
  const observer = object(key) ? submissions.get(key) : undefined;
  if (!observer) return;
  if (object(error)) failures.set(error, observer.origin);
  observer.failed(error);
}
