"""One bounded projector invocation. Started by file path with isolated Python."""

import os
import signal
import sys

INVALID = 20
UNAVAILABLE = 21
TOO_LARGE = 22


def _drop_root_identity():
    # Linux exempts real uid 0 from RLIMIT_NPROC. Some local/Compose Gateways
    # run as root. Only this fresh interpreter relinquishes that identity.
    if os.getuid() != 0:
        return
    import pwd

    nobody = pwd.getpwnam("nobody")
    if nobody.pw_uid <= 0 or nobody.pw_gid <= 0:
        raise ValueError("Unusable unprivileged identity")
    os.setgroups([])
    os.setgid(nobody.pw_gid)
    os.setuid(nobody.pw_uid)
    if (os.getuid(), os.geteuid(), os.getgid(), os.getegid()) != (nobody.pw_uid, nobody.pw_uid, nobody.pw_gid, nobody.pw_gid) or os.getgroups():
        raise ValueError("Preview worker retained privileged credentials")


def _limits(memory_bytes, cpu_seconds):
    if os.name != "posix":
        return
    import resource

    if sys.platform == "linux":
        with open("/proc/self/oom_score_adj", "w", encoding="ascii") as preference:
            preference.write("1000")
    _drop_root_identity()
    signal.signal(signal.SIGXCPU, signal.SIG_DFL)
    for kind, soft, hard in (
        (resource.RLIMIT_AS, memory_bytes, memory_bytes),
        (resource.RLIMIT_CPU, cpu_seconds, cpu_seconds + 1),
        (resource.RLIMIT_FSIZE, 0, 0),
        (resource.RLIMIT_NPROC, 0, 0),
        (resource.RLIMIT_NOFILE, 16, 16),
        (resource.RLIMIT_CORE, 0, 0),
    ):
        resource.setrlimit(kind, (soft, hard))
        if resource.getrlimit(kind) != (soft, hard):
            raise ValueError("Incomplete process limits")
    signal.alarm(12)
    os.nice(19)


def _peak_memory():
    if sys.platform == "linux":
        with open("/proc/self/status", encoding="ascii") as status:
            values = {key: int(value.split()[0]) for key, _, value in (line.partition(":") for line in status) if key in {"VmPeak", "VmHWM"}}
        return values["VmPeak"], values["VmHWM"]
    return 0, 0


def main():
    mode, module_name, function_name, origin, paths, memory, cpu, source_limit = sys.argv[1:]
    try:
        _limits(int(memory), int(cpu))
        if mode == "self-test":
            if os.name == "posix":
                # One finite allocation, only in a short-lived reduced-limit child.
                try:
                    bytearray(int(memory) + 1)
                except MemoryError:
                    pass
                else:
                    return UNAVAILABLE
            return 0
        if mode == "self-test-cpu":
            import hashlib
            import time

            # A normal checksum with a budget expressed in the child's own CPU
            # time. It also finishes on its own if the CPU limit is ineffective.
            finish = time.process_time() + 1.25
            sample = b"preview capability check" * 128
            while time.process_time() < finish:
                hashlib.sha256(sample).digest()
            return UNAVAILABLE
        if mode == "self-test-deadline":
            import time

            # Finite even if the host fails to apply its deliberately short clock.
            time.sleep(2)
            return UNAVAILABLE
        import importlib
        import json

        sys.path[:] = json.loads(paths)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        if os.path.realpath(module.__file__) != origin or function.__module__ != module_name or function.__qualname__ != function_name:
            return UNAVAILABLE
        if mode == "load":
            print(*_peak_memory())
            return 0
        if mode != "project":
            return UNAVAILABLE
    except Exception:
        return UNAVAILABLE
    try:
        raw = sys.stdin.buffer.read(int(source_limit) + 1)
        if len(raw) > int(source_limit):
            return TOO_LARGE
        output = function(raw)
        if not isinstance(output, bytes):
            return INVALID
        # Frame only the return value. Accidental prints from a package never
        # become part of a successful preview.
        sys.stdout.buffer.write(len(output).to_bytes(4, "big"))
        sys.stdout.buffer.write(output)
        sys.stdout.buffer.flush()
        return 0
    except Exception:
        return INVALID


if __name__ == "__main__":
    sys.exit(main())
