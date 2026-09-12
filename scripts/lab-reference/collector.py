#!/usr/bin/env python3
"""Resource collector for one WuKongIM reference launch.

Samples cgroup v2 accounting of named roles at a fixed interval until it is
told to stop (SIGTERM/SIGINT), a sample bound or a deadline is reached, then
writes the comparison document's `resources` material: one entry per role
with cpu_ms, max_rss_bytes, memory_peak_bytes, read_bytes, write_bytes,
samples and throttled_ms; disk free/consumed for the run root; and the Docker
daemon's own process cost when its pid is named. Nothing about the processes
but their accounting is retained: no command lines, no arguments, no paths
other than the output the caller names.

Roles are named as `<role>=cgroup:<absolute cgroup dir>` or
`<role>=pid:<pid>` (the pid's cgroup is resolved once at start). The daemon
is `pid:<pid>`.
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

SCHEMA = "wukongim-reference-collector/1"


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_kv(path):
    values = {}
    try:
        with open(path, encoding="utf-8") as stream:
            for line in stream:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        values[parts[0]] = int(parts[1])
                    except ValueError:
                        continue
    except OSError:
        return None
    return values


def read_int(path):
    try:
        with open(path, encoding="utf-8") as stream:
            text = stream.read().strip()
    except OSError:
        return None
    if text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def read_io(path):
    """Sums rbytes/wbytes over every device line of cgroup v2 io.stat."""
    rbytes = wbytes = 0
    try:
        with open(path, encoding="utf-8") as stream:
            for line in stream:
                for field in line.split()[1:]:
                    key, _, value = field.partition("=")
                    if key == "rbytes":
                        rbytes += int(value)
                    elif key == "wbytes":
                        wbytes += int(value)
    except (OSError, ValueError):
        return None
    return rbytes, wbytes


def cgroup_of_pid(pid, cgroup_root):
    try:
        with open(f"/proc/{pid}/cgroup", encoding="utf-8") as stream:
            for line in stream:
                parts = line.strip().split(":", 2)
                if len(parts) == 3 and parts[0] == "0":
                    return os.path.join(cgroup_root, parts[2].lstrip("/"))
    except OSError:
        return None
    return None


def proc_cost(pid, ticks_per_second):
    """utime+stime in ms and VmHWM in bytes for one pid, or None."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stream:
            stat = stream.read()
        fields = stat[stat.rindex(")") + 2:].split()
        cpu_ticks = int(fields[11]) + int(fields[12])
        hwm = None
        with open(f"/proc/{pid}/status", encoding="utf-8") as stream:
            for line in stream:
                if line.startswith("VmHWM:"):
                    hwm = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError, IndexError):
        return None
    return {"cpu_ms": cpu_ticks * 1000 // ticks_per_second, "max_rss_bytes": hwm or 0}


class Role:
    def __init__(self, name, cgroup):
        self.name = name
        self.cgroup = cgroup
        self.samples = 0
        self.first = None
        self.last = None
        self.max_anon = 0
        self.max_current = 0
        self.peak = None

    def sample(self):
        cpu = read_kv(os.path.join(self.cgroup, "cpu.stat"))
        if cpu is None:
            return False
        io = read_io(os.path.join(self.cgroup, "io.stat"))
        current = read_int(os.path.join(self.cgroup, "memory.current"))
        peak = read_int(os.path.join(self.cgroup, "memory.peak"))
        stat = read_kv(os.path.join(self.cgroup, "memory.stat")) or {}
        point = {
            "usage_usec": cpu.get("usage_usec", 0),
            "throttled_usec": cpu.get("throttled_usec", 0),
            "rbytes": io[0] if io else None,
            "wbytes": io[1] if io else None,
        }
        if self.first is None:
            self.first = point
        self.last = point
        self.samples += 1
        self.max_anon = max(self.max_anon, stat.get("anon", 0))
        if current is not None:
            self.max_current = max(self.max_current, current)
        if peak is not None:
            self.peak = max(self.peak or 0, peak)
        return True

    def summary(self):
        if self.samples == 0 or self.first is None:
            return {"unavailable": "no sample was read for this role"}
        read_bytes = write_bytes = 0
        if self.first["rbytes"] is not None and self.last["rbytes"] is not None:
            read_bytes = max(self.last["rbytes"] - self.first["rbytes"], 0)
            write_bytes = max(self.last["wbytes"] - self.first["wbytes"], 0)
        return {
            "cpu_ms": max(self.last["usage_usec"] - self.first["usage_usec"], 0) // 1000,
            "max_rss_bytes": self.max_anon,
            "memory_peak_bytes": self.peak if self.peak is not None else self.max_current,
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
            "samples": self.samples,
            "throttled_ms": max(self.last["throttled_usec"] - self.first["throttled_usec"], 0) // 1000,
        }


def parse_role(spec, cgroup_root):
    name, _, target = spec.partition("=")
    kind, _, value = target.partition(":")
    if not name or kind not in ("cgroup", "pid") or not value:
        raise SystemExit(f"collector: role {spec!r} is not <role>=cgroup:<dir> or <role>=pid:<pid>")
    if kind == "cgroup":
        if not os.path.isabs(value):
            raise SystemExit(f"collector: cgroup for {name} must be absolute")
        return Role(name, value)
    cgroup = cgroup_of_pid(int(value), cgroup_root)
    if cgroup is None:
        raise SystemExit(f"collector: cgroup of pid for role {name} is unreadable")
    return Role(name, cgroup)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--role", action="append", default=[])
    parser.add_argument("--daemon", default="", help="pid:<dockerd pid>")
    parser.add_argument("--disk-path", required=True)
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup")
    parser.add_argument("--max-samples", type=int, default=0)
    parser.add_argument("--max-seconds", type=int, default=0)
    parser.add_argument("--sample-marker", default="", help="absolute file rewritten with the sample count after every sample")
    args = parser.parse_args(argv)
    if args.interval_ms < 100 or args.interval_ms > 10000:
        raise SystemExit("collector: --interval-ms must be within 100..10000")
    if not args.role:
        raise SystemExit("collector: at least one --role is required")
    if not os.path.isabs(args.output) or not os.path.isabs(args.disk_path):
        raise SystemExit("collector: --output and --disk-path must be absolute")
    if args.sample_marker and not os.path.isabs(args.sample_marker):
        raise SystemExit("collector: --sample-marker must be absolute")
    roles = [parse_role(spec, args.cgroup_root) for spec in args.role]
    names = [role.name for role in roles]
    if len(set(names)) != len(names):
        raise SystemExit("collector: duplicate role name")
    daemon_pid = None
    if args.daemon:
        kind, _, value = args.daemon.partition(":")
        if kind != "pid" or not value.isdigit():
            raise SystemExit("collector: --daemon must be pid:<pid>")
        daemon_pid = int(value)
    ticks = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100

    stop = {"requested": False}

    def request_stop(_signum, _frame):
        stop["requested"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    started = time.monotonic()
    started_utc = utc_now()
    free_at_start = os.statvfs(args.disk_path)
    free_at_start_bytes = free_at_start.f_bavail * free_at_start.f_frsize
    free_min_bytes = free_at_start_bytes
    daemon_first = proc_cost(daemon_pid, ticks) if daemon_pid else None
    daemon_last = daemon_first
    unreadable = []
    samples = 0
    deadline_reached = False
    while not stop["requested"]:
        for role in roles:
            if not role.sample() and role.name not in unreadable:
                unreadable.append(role.name)
        stat = os.statvfs(args.disk_path)
        free_min_bytes = min(free_min_bytes, stat.f_bavail * stat.f_frsize)
        if daemon_pid:
            cost = proc_cost(daemon_pid, ticks)
            if cost is not None:
                daemon_last = cost
        samples += 1
        if args.sample_marker:
            write_atomic(args.sample_marker, f"{samples}\n")
        if args.max_samples and samples >= args.max_samples:
            break
        if args.max_seconds and time.monotonic() - started >= args.max_seconds:
            deadline_reached = True
            break
        # Sleep in short slices so a stop request is honored promptly.
        remaining = args.interval_ms / 1000.0
        while remaining > 0 and not stop["requested"]:
            slice_seconds = min(remaining, 0.05)
            time.sleep(slice_seconds)
            remaining -= slice_seconds
    daemon = {"unavailable": "no daemon pid was named"}
    if daemon_pid:
        if daemon_first is None or daemon_last is None:
            daemon = {"unavailable": "the daemon process was unreadable"}
        else:
            daemon = {
                "cpu_ms": max(daemon_last["cpu_ms"] - daemon_first["cpu_ms"], 0),
                "max_rss_bytes": daemon_last["max_rss_bytes"],
            }
    document = {
        "schema": SCHEMA,
        "started_at_utc": started_utc,
        "ended_at_utc": utc_now(),
        "sample_interval_ms": args.interval_ms,
        "samples": samples,
        "deadline_reached": deadline_reached,
        "stopped_by_signal": stop["requested"],
        "unreadable_roles": unreadable,
        "roles": {role.name: role.summary() for role in roles},
        "disk": {
            "consumed_max_bytes": max(free_at_start_bytes - free_min_bytes, 0),
            "free_at_start_bytes": free_at_start_bytes,
            "free_min_bytes": free_min_bytes,
        },
        "docker_daemon": daemon,
    }
    write_atomic(args.output, json.dumps(document, indent=2, sort_keys=True) + "\n")
    return 0


def write_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as stream:
        stream.write(text)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
