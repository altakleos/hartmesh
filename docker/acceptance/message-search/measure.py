"""Exercise the customer search, chat and delete paths on one disposable stack."""

import argparse
import contextlib
import http.cookiejar
import json
import math
import os
import re
import selectors
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

FIXTURE = Path(__file__).resolve().parent
LIMITS = {
    "message_p95_ms": 1000,
    "catch_up_seconds": 300,
    "delete_overhead_ms": 50,
    "chat_slowdown_ratio": 1.10,
    "footprint": 1.30,
    "lock_seconds": 2,
    "postgres_peak_bytes": 768 * 2**20,
    "title_p95_ms": 300,
}
TIES = {
    "hm_message_search_event_fk": ("hm_message_search_words", "run_events"),
    "hm_message_search_thread_fk": ("hm_message_search_threads", "threads_meta"),
}
PASSWORD = "synthetic-acceptance-password-123"


def p95(samples):
    return sorted(samples)[math.ceil(len(samples) * 0.95) - 1]


def decision(gates):
    if any(g.get("status") == "fail" for g in gates.values()):
        return "miss"
    return "pass" if all(gates.get(key, {}).get("status") == "pass" for key in LIMITS) else "unverified"


def need(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def command(*args, timeout=120, input=None):
    return subprocess.check_output(args, text=True, input=input, timeout=timeout).strip()


class Client:
    def __init__(self, url):
        self.url = url
        self.cookies = http.cookiejar.CookieJar()
        self.http = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))

    def request(self, path, data=None, method=None, timeout=180):
        headers = {"Content-Type": "application/json", "Origin": self.url}
        for cookie in self.cookies:
            if cookie.name == "csrf_token":
                headers["X-CSRF-Token"] = cookie.value
        req = urllib.request.Request(
            self.url + path, data=None if data is None else json.dumps(data).encode(), method=method, headers=headers
        )
        try:
            with self.http.open(req, timeout=timeout) as reply:
                body = reply.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"{req.method if method else req.get_method()} {path} returned HTTP {exc.code}"
            ) from exc
        return json.loads(body) if body else None

    def search(self, query, kind):
        before = time.monotonic()
        response = self.request("/api/search", {"query": query, "types": [kind], "limit": 20})
        return response, (time.monotonic() - before) * 1000

    def turn(self, progress=None):
        thread = self.request("/api/langgraph/threads", {"metadata": {}})["thread_id"]
        before = progress() if progress else None
        start = time.monotonic()
        state = self.request(
            f"/api/langgraph/threads/{thread}/runs/wait",
            {"assistant_id": "lead_agent", "input": {"messages": [{"role": "user", "content": "acceptance:timing"}]}},
        )
        elapsed = (time.monotonic() - start) * 1000
        after = progress() if progress else None
        need(
            any(
                m.get("type") in ("ai", "assistant") and "Timing sample complete." in str(m.get("content"))
                for m in state["messages"]
            ),
            "Scripted model turn did not finish",
        )
        self.request(f"/api/threads/{thread}", method="DELETE")
        return {"ms": elapsed, "markers_before": before, "markers_after": after}


class Measurement:
    def __init__(self, root, gateway, pg):
        self.root, self.gateway, self.pg = root, gateway, pg
        self.url = os.environ["HARTMESH_ACCEPTANCE_URL"]
        self.config = Path(os.environ["ACCEPTANCE_TITLE_CONFIG"])
        self.off = self.config.read_text()
        self.receipt = {
            "schema": 1,
            "limits": LIMITS,
            "gates": {},
            "samples": {},
            "checks": {},
            "limitations": ["Shared runner storage and CPUs; no concurrent tenant sandboxes."],
        }
        self.errors = {}
        self.lock = threading.Lock()

    def save(self):
        with self.lock:
            self.receipt["decision"] = "unverified" if self.errors else decision(self.receipt["gates"])
            self.receipt["errors"] = dict(self.errors)
            path = self.root / "message-search-observations.json"
            tmp = path.with_suffix(".writing")
            tmp.write_text(json.dumps(self.receipt, indent=2) + "\n")
            tmp.replace(path)

    def gate(self, key, value, ok=True):
        self.receipt["gates"][key] = {"observed": value, "status": "pass" if ok and value <= LIMITS[key] else "fail"}
        self.save()

    @contextlib.contextmanager
    def phase(self, name):
        try:
            yield
        except Exception as exc:
            # Public output contains only the checked condition and exception class.
            self.errors[name] = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
            print(f"{name}: {self.errors[name]}", file=sys.stderr, flush=True)
        finally:
            self.save()

    def sql(self, statement):
        return command(
            "docker",
            "exec",
            "-i",
            self.pg,
            "psql",
            "-XqAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "acceptance",
            "-d",
            "acceptance",
            input=statement,
        )

    def seed(self, mode, ids):
        return json.loads(
            command(
                "docker",
                "exec",
                "-w",
                "/app/backend",
                self.gateway,
                "/app/backend/.venv/bin/python",
                "/fixture/seed-message-search.py",
                mode,
                *ids,
                timeout=900,
            )
        )

    def info(self, container, filename):
        return command("docker", "exec", container, "cat", "/sys/fs/cgroup/" + filename)

    def limits(self):
        need(
            self.sql("SELECT current_database(),current_user") == "acceptance|acceptance",
            "Requires the isolated PostgreSQL database and role",
        )
        host = {"architecture": command("uname", "-m"), "logical_cpus": os.cpu_count()}
        for name, container, mb, pids in (("gateway", self.gateway, 1088, 2048), ("postgres", self.pg, 768, 512)):
            details = {
                key: self.info(container, key) for key in ("memory.max", "memory.swap.max", "pids.max", "cpu.max")
            }
            host[name] = details
            need(
                (int(details["memory.max"]), int(details["memory.swap.max"]), int(details["pids.max"]))
                == (mb * 2**20, 0, pids),
                "Unexpected " + name + " cgroup",
            )
            if name == "gateway":
                quota, period = map(int, details["cpu.max"].split())
                need(quota == 2 * period, "Gateway CPU budget does not match distribution")
        self.receipt["platform"] = host
        need(
            self.sql("SHOW synchronous_commit; SHOW wal_writer_delay;").splitlines() == ["off", "200ms"],
            "Database settings differ",
        )
        available = self.sql("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='pg_trgm')")
        allowed = False
        if available == "t":
            try:
                # Transactional creation probes the application role, leaving the extension as found.
                self.sql("BEGIN; CREATE EXTENSION IF NOT EXISTS pg_trgm; ROLLBACK;")
                allowed = True
            except subprocess.CalledProcessError:
                pass
        self.receipt["trigrams"] = {"available": available == "t", "create_succeeded": allowed}
        self.save()

    def restart(self):
        before = time.monotonic()
        command("docker", "restart", "-t", "10", self.gateway, timeout=120)
        while time.monotonic() - before < 140:
            try:
                with urllib.request.urlopen(self.url + "/health/ready", timeout=3) as r:
                    if r.status == 200:
                        return time.monotonic() - before
            except OSError:
                pass
            time.sleep(0.35)
        raise RuntimeError("Gateway did not become ready after restart")

    def enable(self, enabled):
        # Rewrite the existing inode because it is bind-mounted by Docker.
        entry = (FIXTURE / "plugin.yaml").read_text().splitlines()
        self.config.write_text(
            self.off
            + (
                "\nplugins:\n  - " + entry[0] + "\n" + "\n".join("    " + x for x in entry[1:]) + "\n"
                if enabled
                else ""
            )
        )
        return self.restart()

    def browser(self, phase):
        env = dict(
            os.environ,
            HARTMESH_SEARCH_BROWSER_PHASE=phase,
            HARTMESH_ACCEPTANCE_ARTIFACTS=str(self.root / ("browser-" + phase)),
        )
        repo = FIXTURE.parents[2]
        subprocess.run(
            [
                sys.executable,
                str(repo / "scripts/pnpm.py"),
                "--project",
                "frontend-hm",
                "--",
                "exec",
                "playwright",
                "test",
                "--config",
                "playwright.message-search-measurement.config.ts",
            ],
            env=env,
            cwd=repo,
            timeout=150,
            check=True,
        )
        self.receipt["checks"]["browser_" + phase] = True

    def catch_up(self, client, budget, samples):
        start = time.monotonic()
        while time.monotonic() - start < budget:
            result, ms = client.search("saffron ledger witness", "message")
            need(result["messages_available"], "Provider unavailable during catch-up")
            if result["indexing"]:
                need(not result["items"], "Partial search results returned while catching up")
            samples.append(
                {
                    "elapsed_s": round(time.monotonic() - start, 3),
                    "request_ms": round(ms, 3),
                    "indexing": result["indexing"],
                }
            )
            self.save()
            if not result["indexing"]:
                return time.monotonic()
            time.sleep(1)
        raise RuntimeError("Whole history did not finish in the observation window")

    def delete_controls(self, actor, who, phase):
        prepared = self.seed(phase, [who])
        need(prepared["rows"] == 7 * 500, "Incorrect deletion corpus")
        begin = 5000 if phase == "baseline" else 6000
        counts = self.sql(
            f"SELECT count(*) FROM run_events WHERE thread_id BETWEEN 'sample-{begin}' AND 'sample-{begin + 6}';"
        )
        need(int(counts) == 3500, "Expected seven real 500-message histories")
        if phase == "indexed":
            self.catch_up(actor, 360, [])
            need(
                int(
                    self.sql(
                        "SELECT count(*) FROM hm_message_search_words WHERE thread_id BETWEEN 'sample-6000' AND 'sample-6006'"
                    )
                )
                == 3500,
                "Deletion histories are not fully indexed",
            )
        samples = self.receipt["samples"]["delete_" + phase] = []
        for n in range(begin, begin + 7):
            start = time.monotonic()
            actor.request(f"/api/threads/sample-{n}", method="DELETE")
            samples.append((time.monotonic() - start) * 1000)
            self.save()
        need(
            int(
                self.sql(
                    f"SELECT count(*) FROM run_events WHERE thread_id BETWEEN 'sample-{begin}' AND 'sample-{begin + 6}'"
                )
            )
            == 0,
            "Deleted journal rows remained",
        )
        if phase == "indexed":
            need(
                int(
                    self.sql(
                        "SELECT count(*) FROM hm_message_search_words WHERE thread_id BETWEEN 'sample-6000' AND 'sample-6006'"
                    )
                )
                == 0,
                "Deleted search rows remained",
            )
        return samples

    def timed_search(self, person, kind, trials):
        samples = self.receipt["samples"][kind] = []
        for round_no in range(16):
            for query, count, thread in trials:
                result, ms = person.search(query, kind)
                if round_no:
                    samples.append({"query": query, "count": len(result["items"]), "ms": ms})
                    self.save()
                need(
                    result["messages_available"] and not result["indexing"],
                    "Search was unavailable or still catching up",
                )
                need(len(result["items"]) == count, "Wrong result count for " + query)
                if thread:
                    need(result["items"][0]["thread_id"] == thread, "Wrong thread for " + query)
                need(
                    all(
                        i["type"] == kind and i["thread_id"].startswith("sample-") and int(i["thread_id"][7:]) < 2000
                        for i in result["items"]
                    ),
                    "Unexpected result or owner",
                )
        self.gate("message_p95_ms" if kind == "message" else "title_p95_ms", p95([row["ms"] for row in samples]))

    def complete_markers(self, last):
        return int(
            self.sql(
                "SELECT count(*) FROM hm_message_search_threads m JOIN threads_meta t USING(thread_id) "
                "JOIN run_event_thread_seq s USING(thread_id) WHERE m.complete AND m.format=1 "
                "AND m.incarnation=t.incarnation AND m.last_seq=m.target_seq AND s.seq=m.last_seq "
                f"AND m.thread_id BETWEEN 'sample-0000' AND 'sample-{last}'"
            )
        )

    def lock_trials(self, actor):
        rows = self.receipt["samples"]["constraints"] = []
        for constraint, (table, parent) in TIES.items():
            self.sql(f"ALTER TABLE {table} DROP CONSTRAINT {constraint}")
            proc = subprocess.Popen(
                [
                    "docker",
                    "exec",
                    "-i",
                    self.pg,
                    "psql",
                    "-XqAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "acceptance",
                    "-d",
                    "acceptance",
                ],
                text=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            try:
                proc.stdin.write(f"BEGIN; LOCK TABLE {parent} IN ROW EXCLUSIVE MODE; SELECT 'held';\n")
                proc.stdin.flush()
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    need(
                        bool(selector.select(10)) and proc.stdout.readline().strip() == "held",
                        "Failed to acquire controlled writer lock",
                    )
                since = command("date", "-u", "+%Y-%m-%dT%H:%M:%S.%NZ")
                during = self.restart()
                need(
                    actor.request("/api/features")["message_search"] is False, "Timed-out provider remained available"
                )
                need(
                    self.sql(f"SELECT count(*) FROM pg_constraint WHERE conname='{constraint}'") == "0",
                    "Contended constraint installed",
                )
            finally:
                proc.communicate("ROLLBACK;\n", timeout=10)
            retry = self.restart()
            need(actor.request("/api/features")["message_search"] is True, "Provider did not retry at next start")
            need(
                self.sql(f"SELECT count(*) FROM pg_constraint WHERE conname='{constraint}' AND convalidated") == "1",
                "Retried constraint is missing",
            )
            lines = subprocess.run(
                ["docker", "logs", "--since", since, self.gateway],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=30,
                check=True,
            ).stdout
            pattern = rf"Message search constraint {constraint} outcome=(timeout|installed) elapsed_seconds=([0-9.]+)"
            observations = re.findall(pattern, lines)
            need(
                [x[0] for x in observations] == ["timeout", "installed"],
                "Missing exact timeout/retry observations for " + constraint,
            )
            rows.append(
                {
                    "constraint": constraint,
                    "failed_start_s": during,
                    "retry_start_s": retry,
                    "timeout_s": float(observations[0][1]),
                    "installed_s": float(observations[1][1]),
                }
            )
            self.save()
        self.gate("lock_seconds", max(max(r["timeout_s"], r["installed_s"]) for r in rows))

    def run(self):
        self.save()
        self.limits()
        clients = [Client(self.url) for _ in range(20)]
        ids = []
        for index, client in enumerate(clients):
            payload = {"email": f"search-{index}@example.com", "password": PASSWORD}
            # Initialize a different seat first. The heaviest person is an ordinary user.
            if index == 0:
                continue
            ids.append(client.request("/api/v1/auth/" + ("initialize" if index == 1 else "register"), payload)["id"])
        heavy, aux = clients[0], clients[-1]
        ids.insert(
            0, heavy.request("/api/v1/auth/register", {"email": "search-0@example.com", "password": PASSWORD})["id"]
        )
        need(heavy.request("/api/features")["message_search"] is False, "Provider active before opt-in")
        need(
            self.sql(
                "SELECT count(*) FROM pg_class WHERE relname IN ('hm_message_search_words','hm_message_search_threads')"
            )
            == "0",
            "Provider-off start created storage",
        )
        corpus = self.seed("corpus", ids)
        self.receipt["corpus"] = corpus
        summary = self.sql(
            "SELECT count(*),count(DISTINCT user_id),count(DISTINCT thread_id),sum(octet_length(content::json->>'content')) FROM run_events WHERE thread_id LIKE 'sample-%' AND category='message'"
        )
        need(summary == f"800000|20|4000|{corpus['visible_bytes']}", "Durable source corpus differs from input")
        need(
            self.sql(f"SELECT count(*) FROM threads_meta WHERE user_id='{ids[0]}'") == "2000",
            "Heaviest user does not have 2,000 titles",
        )
        self.save()
        with self.phase("off_browser"):
            self.browser("off")
        with self.phase("off_chat"):
            aux.turn()  # model warmup is outside both sets of samples
            samples = self.receipt["samples"]["chat_off"] = []
            for _ in range(7):
                samples.append(aux.turn()["ms"])
                self.save()
        with self.phase("off_delete"):
            self.delete_controls(aux, ids[-1], "baseline")
        self.receipt["startup_s"] = self.enable(True)
        need(heavy.request("/api/features")["message_search"] is True, "Provider startup failed")
        with self.phase("heaviest"):
            aux.turn()  # warm the same model/route after restart without warming heavy's search
            samples = self.receipt["samples"]["catch_up"] = []
            overlapping = self.receipt["samples"]["chat_during_catch_up"] = []
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(self.catch_up, heavy, 900, samples)
                while not samples and not result.done():
                    time.sleep(0.05)
                probes = self.receipt["samples"]["chat_progress"] = []
                while not result.done() and len(overlapping) < 7 and len(probes) < 30:
                    observation = aux.turn(lambda: self.complete_markers(1999))
                    probes.append(observation)
                    if (
                        not result.done()
                        and samples[-1]["indexing"]
                        and observation["markers_before"] < observation["markers_after"] < 2000
                    ):
                        overlapping.append(observation["ms"])
                    self.save()
                finish = result.result()
            need(
                self.sql(
                    "SELECT count(*) FROM hm_message_search_words WHERE thread_id BETWEEN 'sample-0000' AND 'sample-1999'"
                )
                == "400000",
                "Whole heavy history was not indexed",
            )
            self.receipt["corpus"]["heavy_completed_markers"] = self.complete_markers(1999)
            need(self.receipt["corpus"]["heavy_completed_markers"] == 2000, "Heavy history markers incomplete")
            self.gate("catch_up_seconds", finish - start)
            baseline = self.receipt["samples"]["chat_off"]
            need(len(overlapping) >= 3 and len(baseline) == 7, "Insufficient whole overlapping model turns")
            self.gate("chat_slowdown_ratio", statistics.median(overlapping) / statistics.median(baseline))
        with self.phase("footprint"):
            # One authenticated catch-up per seat. Joining each future propagates provider errors.
            end = time.monotonic() + 1200

            def finish_seat(client, report):
                return self.catch_up(client, max(0, min(600, end - time.monotonic())), report)

            with ThreadPoolExecutor(max_workers=3) as pool:
                reports = self.receipt["samples"]["other_seats"] = [[] for _ in clients[1:]]
                tasks = [
                    pool.submit(finish_seat, client, report)
                    for client, report in zip(clients[1:], reports, strict=True)
                ]
                for task in tasks:
                    task.result()
            need(
                self.sql("SELECT count(*) FROM hm_message_search_words WHERE thread_id LIKE 'sample-%'") == "800000",
                "Incomplete indexed corpus",
            )
            self.receipt["corpus"]["completed_markers"] = self.complete_markers(3999)
            need(self.receipt["corpus"]["completed_markers"] == 4000, "Whole history markers incomplete")
            store_bytes = int(
                self.sql(
                    "SELECT pg_total_relation_size('hm_message_search_words')+pg_total_relation_size('hm_message_search_threads')"
                )
            )
            self.receipt["corpus"]["store_bytes_with_indexes"] = store_bytes
            self.gate("footprint", store_bytes / corpus["visible_bytes"])
        with self.phase("search"):
            need(self.receipt["corpus"].get("completed_markers") == 4000, "Whole tenant corpus not indexed")
            self.timed_search(
                heavy,
                "message",
                [
                    ("saffron ledger witness", 1, "sample-0000"),
                    ("violet schedule witness", 1, "sample-1999"),
                    ("Planning item", 20, None),
                    ("foreign meeting witness", 0, None),
                ],
            )
        with self.phase("titles"):
            need(self.receipt["corpus"].get("completed_markers") == 4000, "Whole tenant corpus not indexed")
            self.timed_search(
                heavy,
                "thread",
                [
                    ("Quarterly archive", 1, "sample-0000"),
                    ("Planning notes", 20, "sample-1999"),
                    ("Planning notes 0123", 1, "sample-0123"),
                    ("No matching title", 0, None),
                ],
            )
        with self.phase("active_browser"):
            self.browser("active")
        with self.phase("indexed_delete"):
            indexed = self.delete_controls(aux, ids[-1], "indexed")
            baseline = self.receipt["samples"]["delete_baseline"]
            need(len(baseline) == len(indexed) == 7, "Missing matched deletion observations")
            self.gate("delete_overhead_ms", statistics.median(indexed) - statistics.median(baseline))
        with self.phase("lock"):
            self.lock_trials(heavy)
        with self.phase("off_replay"):
            self.enable(False)
            need(heavy.request("/api/features")["message_search"] is False, "Provider not disabled on replay")
            need(
                self.sql(
                    "SELECT count(*) FROM pg_constraint WHERE conname IN ('hm_message_search_thread_fk','hm_message_search_event_fk') AND convalidated"
                )
                == "2",
                "Recorded constraints disappeared",
            )
            need(
                self.sql(
                    "SELECT count(*) FROM hm_message_search_words WHERE thread_id BETWEEN 'sample-0000' AND 'sample-1999'"
                )
                == "400000",
                "Provider-off replay discarded retained history",
            )
            result, _ = heavy.search("saffron ledger witness", "message")
            need(
                result["messages_available"] is False and result["items"] == [],
                "Off replay exposed cached message hits",
            )
            self.browser("replay")
        with self.phase("memory"):
            peak = int(self.info(self.pg, "memory.peak"))
            events = dict(line.split() for line in self.info(self.pg, "memory.events").splitlines())
            self.receipt["postgres_memory"] = {"peak": peak, "events": events}
            self.gate(
                "postgres_peak_bytes",
                peak,
                int(events["oom"]) == int(events["oom_kill"]) == 0
                and command("docker", "inspect", "--format", "{{.RestartCount}}", self.pg) == "0",
            )
        need(
            not self.errors and set(self.receipt["gates"]) == set(LIMITS),
            "One or more activation measurements could not be completed",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("gateway")
    parser.add_argument("postgres")
    args = parser.parse_args()
    measurement = Measurement(args.artifacts, args.gateway, args.postgres)
    try:
        measurement.run()
    except Exception as error:
        measurement.errors["rehearsal"] = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        raise
    finally:
        measurement.save()
