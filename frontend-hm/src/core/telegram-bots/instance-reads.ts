import {
  isFullInstance,
  listInstances,
  type AgentInstance,
  type Custody,
} from "@/core/agent-instances/api";

/** Complete discovery, never a partial page presented as current access. */
export async function readTelegramInstances(
  custody: Custody,
  owner: AbortSignal,
): Promise<AgentInstance[]> {
  const signal = AbortSignal.any([owner, AbortSignal.timeout(20_000)]);
  const entries = new Map<string, AgentInstance>();
  const seen = new Set<string>();
  for (let offset = 0; ; offset += 100) {
    signal.throwIfAborted();
    const page = await listInstances(custody, offset, signal);
    signal.throwIfAborted();
    if (!Array.isArray(page.instances) || page.instances.length > 100)
      throw new Error("Invalid agent discovery page");
    const previous = seen.size;
    for (const instance of page.instances) {
      seen.add(instance.id);
      if (isFullInstance(instance)) entries.set(instance.id, instance);
    }
    if (page.instances.length < 100) return [...entries.values()];
    if (seen.size === previous)
      throw new Error("Repeated agent discovery page");
  }
}

/** Per-query-client read bound. Retirement removes queued work before it can use a new account's cookie. */
export class InstanceReadPool {
  private active = 0;
  private queue: (() => void)[] = [];
  constructor(private readonly limit = 4) {}
  read<T>(signal: AbortSignal, task: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const abort = () => {
        this.queue = this.queue.filter((entry) => entry !== run);
        reject(new DOMException("Read retired", "AbortError"));
      };
      const run = () => {
        signal.removeEventListener("abort", abort);
        if (signal.aborted) {
          abort();
          return;
        }
        this.active++;
        void (async () => {
          try {
            signal.throwIfAborted();
            const value = await task();
            signal.throwIfAborted();
            resolve(value);
          } catch (error) {
            reject(
              error instanceof Error
                ? error
                : new Error("Instance read failed"),
            );
          } finally {
            this.active--;
            this.drain();
          }
        })();
      };
      if (signal.aborted) {
        abort();
        return;
      }
      signal.addEventListener("abort", abort, { once: true });
      this.queue.push(run);
      this.drain();
    });
  }
  private drain() {
    while (this.active < this.limit && this.queue.length) this.queue.shift()!();
  }
}
