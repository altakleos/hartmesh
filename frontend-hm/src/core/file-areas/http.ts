/**
 * What the two file areas do identically over HTTP.
 *
 * *My files* (`core/files`) and *Shared* (`core/shared`) differ in who may
 * write and who may remove. They do not differ in how a relative path becomes
 * a URL, or how a failed response is turned into something a person can be
 * told — so that lives here once, the way `deerflow.files.store` and
 * `app/gateway/routers/_file_http.py` are shared on the server.
 */

/** A file request that came back with a reason. `detail` is the server's own words. */
export class FileAreaRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string, name = "FileAreaRequestError") {
    super(message);
    this.name = name;
    this.status = status;
  }
}

/** The server's `detail` for a failed response, or *fallback* when it said nothing useful. */
export async function readErrorDetail(
  response: Response,
  fallback: string,
): Promise<string> {
  const data = (await response.json().catch(() => null)) as unknown;
  if (
    typeof data === "object" &&
    data !== null &&
    typeof (data as { detail?: unknown }).detail === "string"
  ) {
    return (data as { detail: string }).detail;
  }
  return fallback;
}

/**
 * Percent-encode each segment but keep the separators, so the path still
 * names the folder it is in. A name carrying `#` or `?` would otherwise cut
 * the URL short and address a different file, or none.
 */
export function encodeRelativePath(path: string) {
  return path.split("/").map(encodeURIComponent).join("/");
}

/**
 * What to tell someone about a failed file request.
 *
 * A refusal the server explained ("Only the person who published this, or an
 * admin, can remove it") is far more use than a generic retry line, and
 * telling someone to try again is wrong when trying again cannot work. So the
 * server's own words win for a refusal or a route that is not there; anything
 * else — a network failure, a server fault — falls back to *retryable*.
 */
export function messageForFileAreaError(
  error: unknown,
  retryable: string,
): string {
  if (error instanceof FileAreaRequestError) {
    // 4xx that a retry cannot fix: say why instead of "try again".
    if (error.status >= 400 && error.status < 500 && error.status !== 408) {
      return error.message;
    }
    if (error.status === 503) return error.message;
  }
  return retryable;
}
