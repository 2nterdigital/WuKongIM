#!/usr/bin/env python3
"""Assembles the launcher provenance of one WuKongIM reference launch.

Reads the fact files the point runner wrote (campaign, source, build, host,
deployment, docker, isolation, resources, cleanup and optionally restart)
and writes one `wukongim-reference-provenance/1` document for
`wkcli bench reference emit`. It computes nothing about the workload: it
projects raw `docker info`/`docker inspect` output and the collector output
into the comparison document's shape, refuses missing or malformed facts,
and never carries a token, a secret-like string or a raw error.
"""

import argparse
import json
import os
import re
import sys

SCHEMA = "wukongim-reference-provenance/1"
COLLECTOR_SCHEMA = "wukongim-reference-collector/1"
ROLE_NAMES = ("node-1", "node-2", "node-3", "client", "coordinator")
SAMPLED_ROLES = ("node-1", "node-2", "node-3", "client")
SECRET_PATTERN = re.compile(r"ghp_|github_pat_|Bearer |token=", re.IGNORECASE)


class Refusal(SystemExit):
    def __init__(self, message):
        super().__init__(f"provenance: {message}")


def load(path, what):
    try:
        with open(path, encoding="utf-8") as stream:
            return json.load(stream)
    except OSError as error:
        raise Refusal(f"{what} is unreadable ({error.strerror})")
    except ValueError:
        raise Refusal(f"{what} is not JSON")


def require(obj, keys, what):
    if not isinstance(obj, dict):
        raise Refusal(f"{what} is not an object")
    missing = [key for key in keys if key not in obj]
    if missing:
        raise Refusal(f"{what} lacks {', '.join(missing)}")
    return obj


def cpuset_count(spec):
    count = 0
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = part.split("-", 1)
            count += int(high) - int(low) + 1
        else:
            count += 1
    return count


def containers_from_inspect(inspect, image_id):
    if not isinstance(inspect, list):
        raise Refusal("docker inspect output is not a list")
    containers = []
    for item in inspect:
        host = item.get("HostConfig") or {}
        cpuset = host.get("CpusetCpus") or ""
        if not cpuset:
            raise Refusal(f"container {item.get('Name', '?')} has no cpuset")
        nano = host.get("NanoCpus") or 0
        quota = nano // 10_000_000 if nano else 100 * cpuset_count(cpuset)
        memory = host.get("Memory") or 0
        if memory <= 0:
            raise Refusal(f"container {item.get('Name', '?')} has no memory limit")
        if item.get("Image") != image_id:
            raise Refusal(f"container {item.get('Name', '?')} runs {item.get('Image')} not the frozen image")
        mounts = []
        for mount in item.get("Mounts") or []:
            kind = mount.get("Type", "")
            if kind not in ("bind", "tmpfs"):
                raise Refusal(f"container {item.get('Name', '?')} mounts a {kind}: only bind and tmpfs are declared")
            mounts.append({"source": mount.get("Source", "") if kind == "bind" else "tmpfs", "destination": mount.get("Destination", ""), "kind": kind})
        containers.append({
            "name": (item.get("Name") or "").lstrip("/"),
            "image_id": item.get("Image"),
            "cpuset": cpuset,
            "cpu_quota_percent": quota,
            "memory_limit_bytes": memory,
            "mounts": mounts,
            "log_path": item.get("LogPath") or "",
        })
    if len(containers) != 3:
        raise Refusal(f"{len(containers)} containers inspected, want exactly three")
    return containers


def topology_from_toml(path):
    """Reads the slot/batching keys of one node TOML without a TOML library."""
    try:
        with open(path, encoding="utf-8") as stream:
            text = stream.read()
    except OSError as error:
        raise Refusal(f"node configuration is unreadable ({error.strerror})")
    section = text[text.index("[cluster]"):] if "[cluster]" in text else ""
    section = section.split("\n[[", 1)[0]

    def key(name, kind):
        match = re.search(rf"^{re.escape(name)}\s*=\s*(\"?)([^\"\n]*)\1\s*$", section, re.MULTILINE)
        if not match:
            raise Refusal(f"node configuration lacks cluster.{name}")
        return int(match.group(2)) if kind is int else match.group(2)

    return {
        "hash_slot_count": key("hash_slot_count", int),
        "initial_slot_count": key("initial_slot_count", int),
        "slot_replica_n": key("slot_replica_n", int),
        "channel_append_batch_max_records": key("channel_append_batch_max_records", int),
        "channel_append_batch_max_wait": key("channel_append_batch_max_wait", str),
        "commit_coordinator_flush_window": key("commit_coordinator_flush_window", str),
        "commit_coordinator_max_requests": key("commit_coordinator_max_requests", int),
        "commit_coordinator_max_records": key("commit_coordinator_max_records", int),
        "commit_coordinator_max_bytes": key("commit_coordinator_max_bytes", int),
        "commit_coordinator_shards": key("commit_coordinator_shards", int),
    }


def check_no_secret(value, where):
    if isinstance(value, dict):
        for key, child in value.items():
            check_no_secret(child, f"{where}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            check_no_secret(child, f"{where}[{index}]")
    elif isinstance(value, str) and SECRET_PATTERN.search(value):
        raise Refusal(f"{where} looks like a credential")


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("campaign", "source", "build", "host", "deployment", "docker-info", "docker-inspect", "isolation", "resources", "cleanup"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--restart", default="")
    parser.add_argument("--node-config", action="append", default=[], help="node TOML files in node order")
    parser.add_argument("--log-volume-bytes", type=int, required=True)
    parser.add_argument("--networks", default="", help="comma-separated project networks after the run")
    parser.add_argument("--volumes", default="", help="comma-separated project volumes after the run")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if not os.path.isabs(args.output):
        raise Refusal("--output must be absolute")

    campaign = require(load(args.campaign, "campaign"), ("change", "manifest_sha256", "launch_sequence", "point_kind", "ladder", "rate_per_second", "repeat_of", "root", "identity_seed", "launched_at_utc", "deadline_ms"), "campaign")
    source = require(load(args.source, "source"), ("repository", "remote_url", "git_ref", "commit", "tree", "parent", "dirty", "dependency_manifest", "planning_base", "instrumentation"), "source")
    build = require(load(args.build, "build"), ("host_toolchain", "profile", "features", "binaries", "images"), "build")
    host = require(load(args.host, "host"), ("teleport_node", "hostname", "labels", "login", "logical_cpus", "memory_bytes", "kernel", "os", "task_root", "backing_device", "filesystem", "mount_options", "free_bytes_at_launch", "clock_synchronized"), "host")
    deployment = require(load(args.deployment, "deployment"), ("role_quotas", "configuration_sha256"), "deployment")
    info = require(load(args.docker_info, "docker info"), ("ServerVersion", "DockerRootDir"), "docker info")
    inspect = load(args.docker_inspect, "docker inspect")
    isolation = require(load(args.isolation, "isolation"), ("quiet_host", "load1_at_launch", "foreign_processes_at_launch", "foreign_compile_seen_during_run", "clock_synchronized", "prior_process_live", "prior_containers_live", "host_overlap"), "isolation")
    resources = require(load(args.resources, "resources"), ("schema", "sample_interval_ms", "roles", "disk", "docker_daemon"), "resources")
    cleanup = require(load(args.cleanup, "cleanup"), ("status", "detail", "residual"), "cleanup")
    restart = load(args.restart, "restart") if args.restart else {"unavailable": "not selected for this root"}

    if resources["schema"] != COLLECTOR_SCHEMA:
        raise Refusal(f"resources schema {resources['schema']!r} is not {COLLECTOR_SCHEMA!r}")
    if set(deployment["role_quotas"]) != set(ROLE_NAMES):
        raise Refusal("deployment.role_quotas must name exactly node-1, node-2, node-3, client and coordinator")
    roles = {}
    for role in SAMPLED_ROLES:
        sample = resources["roles"].get(role)
        if not isinstance(sample, dict) or "unavailable" in sample:
            raise Refusal(f"resources.roles lacks a sample for {role}; missing cgroup evidence is never zero-filled")
        roles[role] = sample
    if not build["images"]:
        raise Refusal("build.images must name the frozen image")
    image_id = build["images"][0].get("image_id", "")
    if not image_id.startswith("sha256:"):
        raise Refusal("build.images[0].image_id must be a sha256 identity")
    if len(args.node_config) != 3:
        raise Refusal("three --node-config files are required in node order")
    topologies = [topology_from_toml(path) for path in args.node_config]
    if any(topology != topologies[0] for topology in topologies[1:]):
        raise Refusal("the three node configurations declare different topologies")
    if set(deployment["configuration_sha256"]) != {"node-1", "node-2", "node-3"}:
        raise Refusal("deployment.configuration_sha256 must name node-1, node-2 and node-3")

    data_root = info["DockerRootDir"]
    exec_root = info.get("ExecRoot") or ""
    socket = info.get("SocketPath") or info.get("ClientInfo", {}).get("Context", "")
    security = info.get("SecurityOptions") or []
    rootless = any("rootless" in str(option) for option in security)
    for path, what in ((data_root, "data root"), (exec_root, "exec root")):
        if not path or not path.startswith(host["task_root"].rstrip("/") + "/"):
            raise Refusal(f"docker {what} {path!r} is not below the task root")
    buildkit = build["images"][0].get("buildkit_version")
    docker = {
        "engine_version": info["ServerVersion"],
        "compose_version": info.get("ClientInfo", {}).get("ComposeVersion") or build.get("compose_version") or "",
        "buildkit_version": buildkit,
        "rootless": rootless,
        "data_root": data_root,
        "exec_root": exec_root,
        "socket": socket,
        "containers": containers_from_inspect(inspect, image_id),
        "volumes": [name for name in args.volumes.split(",") if name],
        "networks": [name for name in args.networks.split(",") if name],
        "daemon_overhead": resources["docker_daemon"],
    }
    if not docker["compose_version"]:
        raise Refusal("the Compose version is unknown; record it in build.compose_version or docker info")

    document = {
        "schema": SCHEMA,
        "campaign": {key: campaign[key] for key in ("change", "manifest_sha256", "launch_sequence", "point_kind", "ladder", "rate_per_second", "repeat_of", "root")},
        "source": source,
        "build": {key: build[key] for key in ("host_toolchain", "profile", "features", "binaries", "images")},
        "host": host,
        "deployment": {
            "role_quotas": deployment["role_quotas"],
            "topology": {"kind": "wukongim_slots", "ech0": None, "wukongim": topologies[0]},
            "configuration_sha256": deployment["configuration_sha256"],
            "docker": docker,
        },
        "isolation": isolation,
        "resources": {
            "sample_interval_ms": resources["sample_interval_ms"],
            "roles": roles,
            "disk": resources["disk"],
            "network": {"unavailable": "loopback traffic is not attributed per role"},
            "log_volume_bytes": args.log_volume_bytes,
            "docker_daemon": resources["docker_daemon"],
        },
        "restart": restart,
        "cleanup": cleanup,
        "identity_seed": campaign["identity_seed"],
        "launched_at_utc": campaign["launched_at_utc"],
        "deadline_ms": campaign["deadline_ms"],
    }
    check_no_secret(document, "provenance")
    encoded = json.dumps(document, indent=2, sort_keys=True) + "\n"
    tmp = args.output + ".tmp"
    with open(tmp, "w", encoding="utf-8") as stream:
        stream.write(encoded)
    os.chmod(tmp, 0o600)
    os.replace(tmp, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
