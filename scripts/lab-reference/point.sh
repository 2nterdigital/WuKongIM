#!/usr/bin/env bash
set -Eeuo pipefail

# Laboratory point runner for one WuKongIM launch of the ech0/WuKongIM
# reference comparison (change establish-ech0-wukongim-reference-load-comparison).
#
# The ech0 campaign coordinator owns the host lease, the launch ledger and the
# product switch; it calls this runner once per WuKongIM root with a fresh run
# root below the task root. The runner, in this order:
#   validate inputs and Docker containment -> refuse any prior product state
#   -> render and gate the Compose configuration -> start exactly three nodes
#   -> wait for readiness -> start the resource collector -> run the wkcli
#   reference client under its own scope and CPU set -> stop the collector
#   -> stop the containers boundedly -> inventory residual state -> assemble
#   provenance -> emit the comparison document -> write the point receipt.
# It never deletes the run root, never removes a volume, never prints a
# credential and never launches a second product. restart.sh sources the
# functions below for the later cold-restart re-audit of a retained root.

readonly LAB_TASK_ROOT_EXPECTED="/srv/tornado-message-data/ecs-user/establish-ech0-wukongim-reference-load-comparison"
readonly LAB_PLANNING_BASE="47bc77ee34d6677a78dd6f8674e4293409e8f9a2"
readonly LAB_NODE_CPUSETS=("0-3" "4-7" "8-11")
readonly LAB_CLIENT_CPUS="12,13"
readonly LAB_CLIENT_CPU_QUOTA="200%"
readonly LAB_API_PORTS=(25001 25002 25003)
readonly LAB_TCP_PORTS=(25100 25101 25102)
readonly LAB_STOP_GRACE="30s"
readonly LAB_DOWN_TIMEOUT_SECONDS=30
readonly LAB_READY_TIMEOUT_SECONDS=120
readonly LAB_COLLECTOR_INTERVAL_MS=1000
readonly LAB_QUIET_LOAD1_MAX="1.00"

LAB_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

die() {
    echo "ERROR: $*" >&2
    return 1
}

log_event() {
    printf 'utc=%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

utc_now() {
    date -u +%Y-%m-%dT%H:%M:%SZ
}

epoch_ms() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

sha256_of() {
    sha256sum -- "$1" | awk '{print $1}'
}

json_field() {
    python3 -c 'import json,sys; v=json.load(open(sys.argv[1]))
for key in sys.argv[2].split("."):
    v = v[int(key)] if isinstance(v, list) else v[key]
print(v if not isinstance(v, bool) else str(v).lower())' "$1" "$2"
}

cpuset_for_node() {
    local node="$1"
    [[ "$node" =~ ^[123]$ ]] || die "node index must be 1, 2 or 3: $node"
    printf '%s\n' "${LAB_NODE_CPUSETS[$((node - 1))]}"
}

api_port_for_node() {
    local node="$1"
    [[ "$node" =~ ^[123]$ ]] || die "node index must be 1, 2 or 3: $node"
    printf '%s\n' "${LAB_API_PORTS[$((node - 1))]}"
}

tcp_port_for_node() {
    local node="$1"
    [[ "$node" =~ ^[123]$ ]] || die "node index must be 1, 2 or 3: $node"
    printf '%s\n' "${LAB_TCP_PORTS[$((node - 1))]}"
}

# validate_run_root <task root> <run root>: the run root is a fresh, absent
# directory directly below <task root>/runs.
validate_run_root() {
    local task_root="$1" run_root="$2"
    [[ "$run_root" == /* ]] || die "run root must be absolute: $run_root"
    [[ "$run_root" != *..* ]] || die "run root must not contain ..: $run_root"
    [[ "$(dirname -- "$run_root")" == "$task_root/runs" ]] || die "run root must be directly below $task_root/runs: $run_root"
    [[ ! -e "$run_root" ]] || die "run root already exists; every launch uses a fresh root: $run_root"
    [[ -d "$task_root/runs" ]] || die "runs directory is missing below the task root"
}

# validate_retained_root <task root> <run root>: an existing point root with
# its document, provenance, acknowledged ledger and stop instant.
validate_retained_root() {
    local task_root="$1" run_root="$2"
    [[ "$run_root" == /* && "$run_root" != *..* ]] || die "run root must be absolute without ..: $run_root"
    [[ "$(dirname -- "$run_root")" == "$task_root/runs" ]] || die "run root must be directly below $task_root/runs: $run_root"
    [[ -d "$run_root" ]] || die "retained run root is missing: $run_root"
    local name
    for name in comparison.json provenance.json client/acknowledged-ledger.jsonl evidence/stopped-at-utc evidence/compose-config.yaml facts/campaign.json facts/source.json facts/build.json facts/host.json facts/deployment.json facts/docker-info.json facts/docker-inspect.json facts/isolation.json facts/resources.json facts/cleanup.json facts/networks.txt; do
        [[ -s "$run_root/$name" ]] || die "retained run root lacks $name"
    done
    [[ ! -e "$run_root/restart" ]] || die "the retained root was already restarted once; a second restart needs its own authorization"
}

validate_task_root() {
    local task_root="$1"
    [[ -d "$task_root" ]] || die "task root is missing: $task_root"
    [[ "$(realpath -- "$task_root")" == "$LAB_TASK_ROOT_EXPECTED" ]] || die "task root is not the authorized root: $task_root"
}

below_task_root() {
    local task_root="$1" path="$2"
    [[ "$path" == "$task_root"/* && "$path" != *..* ]]
}

# compose_env <run root> <conf dir> <image> <project> <metrics on|off> <memory> <user>
# prints the exact variables the laboratory Compose file requires, one per
# line, so the same set feeds `config`, `up` and `down`.
compose_env() {
    local run_root="$1" conf_dir="$2" image="$3" project="$4" metrics="$5" memory="$6" user="$7"
    case "$metrics" in
        on) metrics=true ;;
        off) metrics=false ;;
        *) die "instrumentation must be on or off: $metrics" ;;
    esac
    printf 'WK_LAB_PROJECT=%s\n' "$project"
    printf 'WK_LAB_IMAGE=%s\n' "$image"
    printf 'WK_LAB_CONTAINER_USER=%s\n' "$user"
    printf 'WK_LAB_STOP_GRACE=%s\n' "$LAB_STOP_GRACE"
    printf 'WK_LAB_NODE_MEMORY=%s\n' "$memory"
    printf 'WK_LAB_METRICS_ENABLE=%s\n' "$metrics"
    printf 'WK_LAB_CONF_DIR=%s\n' "$conf_dir"
    printf 'WK_LAB_ROOT=%s\n' "$run_root"
    local node
    for node in 1 2 3; do
        printf 'WK_LAB_NODE%s_CPUSET=%s\n' "$node" "$(cpuset_for_node "$node")"
        printf 'WK_LAB_NODE%s_API_PORT=%s\n' "$node" "$(api_port_for_node "$node")"
        printf 'WK_LAB_NODE%s_TCP_PORT=%s\n' "$node" "$(tcp_port_for_node "$node")"
    done
}

# gate_compose_config <rendered config> <run root> <conf dir>: every bind
# source is the configuration file or a directory below the run root; no
# volume, no fourth service.
gate_compose_config() {
    local rendered="$1" run_root="$2" conf_dir="$3"
    python3 - "$rendered" "$run_root" "$conf_dir" <<'PY'
import sys

rendered, run_root, conf_dir = sys.argv[1:]
services = 0
binds = 0
with open(rendered, encoding="utf-8") as stream:
    lines = stream.read().splitlines()
top_level = [line for line in lines if line and not line[0].isspace()]
for line in top_level:
    if line.startswith("volumes:"):
        raise SystemExit("rendered Compose declares a top-level volume")
for index, line in enumerate(lines):
    if line.startswith("  wk-node") and line.endswith(":"):
        services += 1
    stripped = line.strip()
    if stripped.startswith("- "):
        stripped = stripped[2:]
    if stripped.startswith("source: "):
        source = stripped[len("source: "):]
        binds += 1
        ok = source.startswith(run_root + "/") or (source.startswith(conf_dir + "/") and source.endswith(".toml"))
        if not ok:
            raise SystemExit(f"bind source escapes the run root: {source}")
    if stripped.startswith("type: ") and stripped not in ("type: bind",):
        raise SystemExit(f"only bind mounts are declared: {stripped}")
if services != 3:
    raise SystemExit(f"rendered Compose declares {services} node services, want 3")
if binds != 9:
    raise SystemExit(f"rendered Compose declares {binds} bind mounts, want 9")
PY
}

# docker_containment <docker context> <task root> <allow rootful 0|1> <raw info output>
# proves the daemon's data root, exec root and socket lie below the task root
# and prints: data_root, exec_root, socket, rootless, dockerd_pid (one per line).
docker_containment() {
    local docker_context="$1" task_root="$2" allow_rootful="$3" raw="$4"
    docker --context "$docker_context" info --format '{{json .}}' >"$raw" 2>/dev/null || die "docker info failed for context $docker_context"
    local data_root rootless dockerd_pid exec_root socket_path
    data_root="$(json_field "$raw" DockerRootDir)"
    rootless="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("true" if any("rootless" in str(o) for o in d.get("SecurityOptions") or []) else "false")' "$raw")"
    dockerd_pid="$(pgrep -u "$(id -u)" -x dockerd | head -n1 || true)"
    [[ -n "$dockerd_pid" ]] || die "no dockerd of this user is running; a task-scoped daemon is required"
    exec_root="$(tr '\0' '\n' </proc/"$dockerd_pid"/cmdline | awk -F= '/^--exec-root=/{print $2}' | head -n1)"
    [[ -n "$exec_root" ]] || exec_root="$(tr '\0' '\n' </proc/"$dockerd_pid"/cmdline | awk '/^--exec-root$/{getline; print}' | head -n1)"
    socket_path="$(docker context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')"
    below_task_root "$task_root" "$data_root" || die "docker data root escapes the task root: $data_root"
    below_task_root "$task_root" "$exec_root" || die "docker exec root escapes the task root: ${exec_root:-unset}"
    [[ "$socket_path" == unix://"$task_root"/* ]] || die "docker socket escapes the task root: $socket_path"
    if [[ "$rootless" != true && "$allow_rootful" != 1 ]]; then
        die "the daemon is not rootless; pass --allow-authorized-rootful only for an owner-authorized contained daemon"
    fi
    printf '%s\n%s\n%s\n%s\n%s\n' "$data_root" "$exec_root" "$socket_path" "$rootless" "$dockerd_pid"
}

# refuse_prior_state <docker context> <wkcli>: no container, product process
# or laboratory port may be live before a launch.
refuse_prior_state() {
    local docker_context="$1" wkcli="$2"
    local prior_containers prior_processes port
    prior_containers="$(docker --context "$docker_context" ps -q | wc -l | tr -d ' ')"
    prior_processes="$(pgrep -u "$(id -u)" -f -- "bench reference run|/usr/local/bin/wukongim -config|integration-server-cli run node.lab" | wc -l | tr -d ' ')"
    [[ "$prior_containers" == 0 ]] || die "$prior_containers container(s) are live before launch; the previous product was not cleaned"
    [[ "$prior_processes" == 0 ]] || die "$prior_processes product process(es) are live before launch"
    for port in "${LAB_API_PORTS[@]}" "${LAB_TCP_PORTS[@]}"; do
        if ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE ":$port$"; then
            die "port $port is already bound"
        fi
    done
}

# host_quietness prints: quiet(true|false), load1, foreign(comma list), clock_synchronized(true|false).
host_quietness() {
    local load1 quiet=true foreign clock=false
    load1="$(awk '{print $1}' /proc/loadavg)"
    foreign="$(ps -eo stat=,comm= | awk '$1 ~ /^R/ && $2 != "ps" && $2 != "awk" {print $2}' | sort -u | tr '\n' ',' | sed 's/,$//')"
    if [[ -n "$foreign" ]] || awk -v a="$load1" -v b="$LAB_QUIET_LOAD1_MAX" 'BEGIN{exit !(a > b)}'; then
        quiet=false
    fi
    if command -v timedatectl >/dev/null 2>&1 && [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" == yes ]]; then
        clock=true
    fi
    printf '%s\n%s\n%s\n%s\n' "$quiet" "$load1" "$foreign" "$clock"
}

# verify_frozen_identities <source dir> <source.json> <build.json> <image> <docker context> <wkcli>
verify_frozen_identities() {
    local source_dir="$1" source_file="$2" build_file="$3" image="$4" docker_context="$5" wkcli="$6"
    local expected_commit head_commit expected_image_id actual_image_id expected_wkcli_sha
    expected_commit="$(json_field "$source_file" commit)"
    head_commit="$(git -C "$source_dir" rev-parse HEAD)"
    [[ "$head_commit" == "$expected_commit" ]] || die "source dir HEAD $head_commit is not the frozen commit $expected_commit"
    [[ -z "$(git -C "$source_dir" status --porcelain)" ]] || die "source dir is dirty"
    expected_image_id="$(json_field "$build_file" images.0.image_id)"
    actual_image_id="$(docker --context "$docker_context" image inspect --format '{{.Id}}' "$image")"
    [[ "$actual_image_id" == "$expected_image_id" ]] || die "image $image is $actual_image_id, not the frozen $expected_image_id"
    expected_wkcli_sha="$(python3 -c 'import json,sys; print([b for b in json.load(open(sys.argv[1]))["binaries"] if b["name"]=="wkcli"][0]["sha256"])' "$build_file")"
    [[ "$(sha256_of "$wkcli")" == "$expected_wkcli_sha" ]] || die "wkcli sha256 differs from the frozen build"
}

# wait_nodes_ready <timeout seconds>: every node answers /readyz on its port.
wait_nodes_ready() {
    local timeout="$1" node port deadline ready
    for node in 1 2 3; do
        port="$(api_port_for_node "$node")"
        deadline=$((SECONDS + timeout))
        ready=0
        while (( SECONDS < deadline )); do
            if curl -fsS -m 3 "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; then
                ready=1
                break
            fi
            sleep 1
        done
        (( ready == 1 )) || die "node $node did not become ready within ${timeout}s"
    done
}

# inspect_project <docker context> <project> <facts dir>: records the three
# containers, the project network and refuses any project volume.
inspect_project() {
    local docker_context="$1" project="$2" facts="$3"
    docker --context "$docker_context" ps --filter "label=com.docker.compose.project=$project" -q | xargs docker --context "$docker_context" inspect >"$facts/docker-inspect.json"
    docker --context "$docker_context" network ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}' >"$facts/networks.txt"
    docker --context "$docker_context" volume ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}' >"$facts/volumes.txt"
    [[ ! -s "$facts/volumes.txt" ]] || die "the project created a volume; only bind mounts are declared"
}

# node_role_args <docker context> <project>: prints --role node-N=cgroup:<dir> pairs.
node_role_args() {
    local docker_context="$1" project="$2" node container_pid cgroup_rel
    for node in 1 2 3; do
        container_pid="$(docker --context "$docker_context" inspect --format '{{.State.Pid}}' "$project-wk-node$node-1")"
        cgroup_rel="$(awk -F: '$1=="0"{print $3}' /proc/"$container_pid"/cgroup)"
        [[ -n "$cgroup_rel" ]] || die "cgroup of node $node is unreadable"
        printf -- '--role\nnode-%s=cgroup:/sys/fs/cgroup%s\n' "$node" "$cgroup_rel"
    done
}

# compose_down_and_inventory <docker context> <compose file> <project> <run root> <evidence dir> <facts dir> <wkcli> <compose var>...
# stops the containers boundedly, then inventories every residual kind and
# writes <facts dir>/cleanup.json; prints clean or failed.
compose_down_and_inventory() {
    local docker_context="$1" compose_file="$2" project="$3" run_root="$4" evidence="$5" facts="$6" wkcli="$7"
    shift 7
    log_event "stopping containers of $project with a ${LAB_DOWN_TIMEOUT_SECONDS}s grace"
    env "$@" docker --context "$docker_context" compose -f "$compose_file" down --remove-orphans --timeout "$LAB_DOWN_TIMEOUT_SECONDS" \
        >"$evidence/compose-down.log" 2>&1 || log_event "compose down reported a failure; the inventory decides"
    utc_now >"$evidence/stopped-at-utc"
    docker --context "$docker_context" ps -a --filter "label=com.docker.compose.project=$project" --format '{{.ID}} {{.Names}} {{.Status}}' >"$evidence/residual-containers.txt" 2>/dev/null || true
    docker --context "$docker_context" network ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}' >"$evidence/residual-networks.txt" 2>/dev/null || true
    docker --context "$docker_context" volume ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}' >"$evidence/residual-volumes.txt" 2>/dev/null || true
    findmnt -rn -o TARGET 2>/dev/null | { grep -F -- "$run_root/" || true; } >"$evidence/residual-mounts.txt"
    ss -ltnH 2>/dev/null | awk '{print $4}' | { grep -E ":(25001|25002|25003|25100|25101|25102)$" || true; } >"$evidence/residual-ports.txt"
    pgrep -u "$(id -u)" -f -- "$wkcli bench reference (run|reaudit)|/usr/local/bin/wukongim -config" >"$evidence/residual-processes.txt" 2>/dev/null || true
    systemctl --user list-units --all --plain --no-legend "wk-ref-client-*" 2>/dev/null | awk '{print $1}' >"$evidence/residual-cgroups.txt" || true
    local open_files
    open_files="$(find /proc/[0-9]*/fd -lname "$run_root/*" 2>/dev/null | { grep -Ev "^/proc/($$|$BASHPID)/" || true; })"
    printf '%s\n' "$open_files" >"$evidence/residual-open-files.txt"
    python3 - "$facts/cleanup.json" "$evidence" <<'PY'
import json, os, sys
output, evidence = sys.argv[1:]
counts = {}
for key, name in (("containers", "residual-containers.txt"), ("networks", "residual-networks.txt"), ("volumes", "residual-volumes.txt"),
                  ("mounts", "residual-mounts.txt"), ("ports", "residual-ports.txt"), ("processes", "residual-processes.txt"),
                  ("cgroups", "residual-cgroups.txt"), ("open_files", "residual-open-files.txt")):
    try:
        with open(os.path.join(evidence, name), encoding="utf-8") as stream:
            counts[key] = len([line for line in stream if line.strip()])
    except OSError:
        counts[key] = 0
residual = {key: counts.get(key, 0) for key in ("processes", "containers", "ports", "cgroups", "networks", "mounts", "volumes", "open_files")}
clean = all(value == 0 for value in residual.values())
detail = None if clean else "residual " + ", ".join(f"{key}={value}" for key, value in residual.items() if value)
os.makedirs(os.path.dirname(output), exist_ok=True)
with open(output, "w", encoding="utf-8") as stream:
    json.dump({"status": "clean" if clean else "failed", "detail": detail, "residual": residual}, stream, indent=2, sort_keys=True)
    stream.write("\n")
os.chmod(output, 0o600)
print("clean" if clean else "failed")
PY
}

# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

usage() {
    cat <<'USAGE'
Usage: point.sh --task-root DIR --run-root DIR --source-dir DIR --wkcli FILE --image NAME
                --campaign-file FILE --source-file FILE --build-file FILE --host-file FILE
                --instrumentation on|off --docker-context NAME --deadline-seconds N
                [--node-memory 8g] [--client-memory-max 4G] [--container-user 0:0]
                [--coordinator-cgroup DIR] [--allow-authorized-rootful]

Runs exactly one WuKongIM reference point on the laboratory host. Every path
must lie below the authorized task root; the run root must not exist yet.
USAGE
}

main() {
    local task_root="" run_root="" source_dir="" wkcli="" image="" campaign_file="" source_file="" build_file="" host_file=""
    local instrumentation="" docker_context="" deadline_seconds="" node_memory="8g" client_memory_max="4G"
    local container_user="0:0" coordinator_cgroup="" allow_rootful=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --task-root) task_root="$2"; shift 2 ;;
            --run-root) run_root="$2"; shift 2 ;;
            --source-dir) source_dir="$2"; shift 2 ;;
            --wkcli) wkcli="$2"; shift 2 ;;
            --image) image="$2"; shift 2 ;;
            --campaign-file) campaign_file="$2"; shift 2 ;;
            --source-file) source_file="$2"; shift 2 ;;
            --build-file) build_file="$2"; shift 2 ;;
            --host-file) host_file="$2"; shift 2 ;;
            --instrumentation) instrumentation="$2"; shift 2 ;;
            --docker-context) docker_context="$2"; shift 2 ;;
            --deadline-seconds) deadline_seconds="$2"; shift 2 ;;
            --node-memory) node_memory="$2"; shift 2 ;;
            --client-memory-max) client_memory_max="$2"; shift 2 ;;
            --container-user) container_user="$2"; shift 2 ;;
            --coordinator-cgroup) coordinator_cgroup="$2"; shift 2 ;;
            --allow-authorized-rootful) allow_rootful=1; shift ;;
            -h|--help) usage; return 0 ;;
            *) usage >&2; die "unknown argument: $1" ;;
        esac
    done
    local name
    for name in task_root run_root source_dir wkcli image campaign_file source_file build_file host_file instrumentation docker_context deadline_seconds; do
        [[ -n "${!name}" ]] || die "--${name//_/-} is required"
    done
    [[ "$deadline_seconds" =~ ^[0-9]+$ && "$deadline_seconds" -gt 0 ]] || die "--deadline-seconds must be a positive integer"
    for name in docker python3 curl ss sha256sum realpath systemd-run taskset findmnt awk git; do
        require_command "$name"
    done
    validate_task_root "$task_root"
    validate_run_root "$task_root" "$run_root"
    below_task_root "$task_root" "$source_dir" || die "source dir must be below the task root"
    below_task_root "$task_root" "$wkcli" || die "wkcli must be below the task root"
    [[ -x "$wkcli" ]] || die "wkcli is not executable"
    local compose_file="$source_dir/docker/lab/reference/compose.yml"
    local conf_dir="$source_dir/docker/lab/reference/conf"
    [[ -f "$compose_file" ]] || die "laboratory Compose file is missing in the source dir"
    [[ -f "$campaign_file" && -f "$source_file" && -f "$build_file" && -f "$host_file" ]] || die "a fact file is missing"
    [[ "$instrumentation" == on || "$instrumentation" == off ]] || die "--instrumentation must be on or off"

    local sequence rate seed run_id project
    sequence="$(json_field "$campaign_file" launch_sequence)"
    rate="$(json_field "$campaign_file" rate_per_second)"
    seed="$(json_field "$campaign_file" identity_seed)"
    run_id="$(printf 'ref-seq%02d' "$sequence")"
    project="$(printf 'wk-ref-seq%02d' "$sequence")"

    local facts="$run_root/facts" evidence="$run_root/evidence" client_dir="$run_root/client"
    local receipt="$run_root/point-receipt.json"
    local phase="validate" client_exit="" cleanup_status="" collector_pid="" client_pid="" started_utc created_root=0 launched=0
    local -a compose_vars=()
    mapfile -t compose_vars < <(compose_env "$run_root" "$conf_dir" "$image" "$project" "$instrumentation" "$node_memory" "$container_user")
    (( ${#compose_vars[@]} == 17 )) || die "compose environment is incomplete"
    started_utc="$(utc_now)"
    finish() {
        local status=$?
        trap - EXIT
        if [[ -n "$collector_pid" ]] && kill -0 "$collector_pid" 2>/dev/null; then
            kill -TERM "$collector_pid" 2>/dev/null || true
            wait "$collector_pid" 2>/dev/null || true
        fi
        if [[ -z "$cleanup_status" && "$launched" == 1 ]]; then
            cleanup_status="$(compose_down_and_inventory "$docker_context" "$compose_file" "$project" "$run_root" "$evidence" "$facts" "$wkcli" "${compose_vars[@]}" || echo failed)"
        fi
        if [[ "$created_root" == 1 ]]; then
            write_point_receipt "$receipt" "$status" "$phase" "$sequence" "$rate" "$run_root" "$client_exit" "$cleanup_status" "$started_utc"
        fi
        exit "$status"
    }
    trap finish EXIT

    # --- fresh root -------------------------------------------------------------
    phase="prepare"
    mkdir -p -- "$run_root"/node{1,2,3}/{data,logs} "$client_dir" "$facts" "$evidence"
    chmod 0700 -- "$run_root"
    created_root=1

    # --- containment and prior state ----------------------------------------
    phase="containment"
    local -a containment=()
    mapfile -t containment < <(docker_containment "$docker_context" "$task_root" "$allow_rootful" "$facts/docker-info.raw.json")
    (( ${#containment[@]} == 5 )) || die "docker containment could not be proven"
    local exec_root="${containment[1]}" socket_path="${containment[2]}" dockerd_pid="${containment[4]}"
    refuse_prior_state "$docker_context" "$wkcli"
    local -a quietness=()
    mapfile -t quietness < <(host_quietness)

    # --- facts ---------------------------------------------------------------------
    phase="facts"
    python3 - "$facts/docker-info.raw.json" "$facts/docker-info.json" "$exec_root" "$socket_path" "$(docker --context "$docker_context" compose version --short 2>/dev/null || echo unknown)" <<'PY'
import json, sys
raw, output, exec_root, socket_path, compose_version = sys.argv[1:]
info = json.load(open(raw, encoding="utf-8"))
info["ExecRoot"] = exec_root
info["SocketPath"] = socket_path
info.setdefault("ClientInfo", {})["ComposeVersion"] = "v" + compose_version if compose_version and not compose_version.startswith("v") else compose_version
json.dump(info, open(output, "w", encoding="utf-8"), indent=2, sort_keys=True)
PY
    python3 - "$facts/isolation.json" "${quietness[0]}" "${quietness[1]}" "${quietness[2]}" "${quietness[3]}" <<'PY'
import json, sys
output, quiet, load1, foreign, clock = sys.argv[1:]
json.dump({
    "quiet_host": quiet == "true",
    "load1_at_launch": float(load1),
    "foreign_processes_at_launch": [name for name in foreign.split(",") if name],
    "foreign_compile_seen_during_run": False,
    "clock_synchronized": clock == "true",
    "prior_process_live": False,
    "prior_containers_live": False,
    "host_overlap": False,
}, open(output, "w", encoding="utf-8"), indent=2, sort_keys=True)
PY
    cp -- "$campaign_file" "$facts/campaign.json"
    cp -- "$source_file" "$facts/source.json"
    cp -- "$build_file" "$facts/build.json"
    cp -- "$host_file" "$facts/host.json"
    # The frozen source and image are re-verified, never re-derived.
    verify_frozen_identities "$source_dir" "$source_file" "$build_file" "$image" "$docker_context" "$wkcli"
    python3 - "$facts/deployment.json" "$conf_dir" "$node_memory" <<'PY'
import hashlib, json, os, sys
output, conf_dir, node_memory = sys.argv[1:]
units = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
memory = node_memory.strip().lower()
memory_bytes = int(memory[:-1]) * units[memory[-1]] if memory[-1] in units else int(memory)
def role(cpus):
    return {"cpus": cpus, "cpu_quota_percent": 100 * len(cpus), "memory_max_bytes": memory_bytes, "cgroup": None}
quotas = {"node-1": role([0, 1, 2, 3]), "node-2": role([4, 5, 6, 7]), "node-3": role([8, 9, 10, 11]), "client": role([12, 13]), "coordinator": role([14, 15])}
digests = {}
for n in (1, 2, 3):
    with open(os.path.join(conf_dir, f"node{n}.toml"), "rb") as stream:
        digests[f"node-{n}"] = hashlib.sha256(stream.read()).hexdigest()
json.dump({"role_quotas": quotas, "configuration_sha256": digests}, open(output, "w", encoding="utf-8"), indent=2, sort_keys=True)
PY

    # --- Compose gate and launch ---------------------------------------------
    phase="compose-config"
    env "${compose_vars[@]}" \
        docker --context "$docker_context" compose -f "$compose_file" config >"$evidence/compose-config.yaml"
    gate_compose_config "$evidence/compose-config.yaml" "$run_root" "$conf_dir"
    phase="compose-up"
    log_event "starting $project (sequence $sequence, ${rate}/s, instrumentation $instrumentation)"
    launched=1
    env "${compose_vars[@]}" \
        docker --context "$docker_context" compose -f "$compose_file" up -d --no-build --no-recreate >"$evidence/compose-up.log" 2>&1
    phase="ready"
    wait_nodes_ready "$LAB_READY_TIMEOUT_SECONDS"
    inspect_project "$docker_context" "$project" "$facts"
    local -a role_args=()
    mapfile -t role_args < <(node_role_args "$docker_context" "$project")
    if [[ -n "$coordinator_cgroup" ]]; then
        role_args+=("--role" "coordinator=cgroup:$coordinator_cgroup")
    fi

    # --- client under its own scope, collector first --------------------------
    phase="client"
    local unit
    unit="wk-ref-client-$(printf '%02d' "$sequence")"
    local go_file="$client_dir/go" cgroup_file="$client_dir/cgroup" deadline
    local -a servers=() gateways=()
    local node
    for node in 1 2 3; do
        servers+=("http://127.0.0.1:$(api_port_for_node "$node")")
        gateways+=("127.0.0.1:$(tcp_port_for_node "$node")")
    done
    systemd-run --user --scope --quiet --unit="$unit" -p MemoryMax="$client_memory_max" -p MemorySwapMax=0 -p CPUQuota="$LAB_CLIENT_CPU_QUOTA" -- \
        taskset -c "$LAB_CLIENT_CPUS" bash -c '
            set -euo pipefail
            awk -F: '"'"'$1=="0"{print "/sys/fs/cgroup" $3}'"'"' /proc/self/cgroup > "$1"
            while [[ ! -e "$2" ]]; do sleep 0.1; done
            shift 2
            exec "$@"
        ' _ "$cgroup_file" "$go_file" "$wkcli" bench reference run \
            --server "${servers[0]}" --server "${servers[1]}" --server "${servers[2]}" \
            --gateway "${gateways[0]}" --gateway "${gateways[1]}" --gateway "${gateways[2]}" \
            --rate "$rate" --seed "$seed" --run-id "$run_id" --instrumentation "$instrumentation" \
            --ledger "$client_dir/acknowledged-ledger.jsonl" \
            --output "$client_dir/reference-run.json" >"$client_dir/wkcli.log" 2>&1 &
    client_pid=$!
    deadline=$((SECONDS + 30))
    while [[ ! -s "$cgroup_file" ]] && (( SECONDS < deadline )); do sleep 0.1; done
    [[ -s "$cgroup_file" ]] || die "the client scope did not report its cgroup"
    role_args+=("--role" "client=cgroup:$(cat "$cgroup_file")")
    python3 "$LAB_SCRIPT_DIR/collector.py" --output "$facts/resources.json" --disk-path "$run_root" \
        --interval-ms "$LAB_COLLECTOR_INTERVAL_MS" --daemon "pid:$dockerd_pid" --max-seconds "$deadline_seconds" \
        --sample-marker "$facts/collector.marker" "${role_args[@]}" >"$evidence/collector.log" 2>&1 &
    collector_pid=$!
    deadline=$((SECONDS + 15))
    while [[ ! -s "$facts/collector.marker" ]] && (( SECONDS < deadline )); do sleep 0.1; done
    [[ -s "$facts/collector.marker" ]] || die "the collector took no sample"
    : >"$go_file"
    log_event "client released under $unit; deadline ${deadline_seconds}s"
    deadline=$((SECONDS + deadline_seconds))
    while kill -0 "$client_pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 1; done
    if kill -0 "$client_pid" 2>/dev/null; then
        log_event "client exceeded the deadline; stopping the scope"
        systemctl --user stop "$unit.scope" 2>/dev/null || kill -TERM "$client_pid" 2>/dev/null || true
        wait "$client_pid" || true
        client_exit=124
    else
        set +e
        wait "$client_pid"
        client_exit=$?
        set -e
    fi
    log_event "client exit code $client_exit"
    kill -TERM "$collector_pid" 2>/dev/null || true
    wait "$collector_pid" || true
    collector_pid=""
    [[ -s "$facts/resources.json" ]] || die "the collector wrote no resources"

    # --- log volume, teardown, inventory ----------------------------------------
    phase="log-volume"
    local log_volume_bytes
    log_volume_bytes="$(python3 - "$run_root" "$facts/docker-inspect.json" <<'PY'
import json, os, sys
run_root, inspect = sys.argv[1:]
total = 0
for n in (1, 2, 3):
    for base, _dirs, files in os.walk(os.path.join(run_root, f"node{n}", "logs")):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(base, name))
            except OSError:
                pass
for item in json.load(open(inspect, encoding="utf-8")):
    path = item.get("LogPath") or ""
    try:
        total += os.path.getsize(path)
    except OSError:
        pass
client_log = os.path.join(run_root, "client", "wkcli.log")
total += os.path.getsize(client_log) if os.path.isfile(client_log) else 0
print(total)
PY
)"
    printf '%s\n' "$log_volume_bytes" >"$facts/log-volume-bytes"
    phase="teardown"
    cleanup_status="$(compose_down_and_inventory "$docker_context" "$compose_file" "$project" "$run_root" "$evidence" "$facts" "$wkcli" "${compose_vars[@]}")"
    log_event "cleanup $cleanup_status"

    # --- provenance and document --------------------------------------------------
    phase="provenance"
    python3 "$LAB_SCRIPT_DIR/provenance.py" --campaign "$facts/campaign.json" --source "$facts/source.json" --build "$facts/build.json" \
        --host "$facts/host.json" --deployment "$facts/deployment.json" --docker-info "$facts/docker-info.json" \
        --docker-inspect "$facts/docker-inspect.json" --isolation "$facts/isolation.json" --resources "$facts/resources.json" \
        --cleanup "$facts/cleanup.json" --node-config "$conf_dir/node1.toml" --node-config "$conf_dir/node2.toml" --node-config "$conf_dir/node3.toml" \
        --log-volume-bytes "$log_volume_bytes" --networks "$(paste -sd, "$facts/networks.txt" 2>/dev/null || true)" --volumes "" \
        --output "$run_root/provenance.json"
    phase="emit"
    [[ -s "$client_dir/reference-run.json" ]] || die "the client wrote no run result (exit $client_exit)"
    "$wkcli" bench reference emit --run "$client_dir/reference-run.json" --provenance "$run_root/provenance.json" --output "$run_root/comparison.json" >"$evidence/emit.log" 2>&1
    phase="done"
    [[ "$client_exit" == 0 ]] || die "the client exited $client_exit; the document is written and the receipt records the failure"
    [[ "$cleanup_status" == clean ]] || die "cleanup left residual state; the next launch is blocked until an isolation proof"
    log_event "point $sequence complete: $run_root/comparison.json"
}

# write_point_receipt <receipt> <status> <phase> <sequence> <rate> <run root> <client exit> <cleanup status> <started utc>
write_point_receipt() {
    python3 - "$@" "$(utc_now)" <<'PY'
import hashlib, json, os, sys
receipt, status, phase, sequence, rate, run_root, client_exit, cleanup_status, started, ended = sys.argv[1:]
document = {
    "schema": "wukongim-reference-point-receipt/1",
    "product": "wukongim",
    "launch_sequence": int(sequence),
    "rate_per_second": int(rate),
    "run_root": run_root,
    "result": "pass" if status == "0" else "fail",
    "exit_code": int(status),
    "failure_phase": None if status == "0" else phase,
    "client_exit_code": int(client_exit) if client_exit else None,
    "cleanup_status": cleanup_status or "not_reached",
    "started_at_utc": started,
    "ended_at_utc": ended,
}
for name in ("comparison.json", "client/reference-run.json", "client/acknowledged-ledger.jsonl"):
    path = os.path.join(run_root, name)
    if os.path.isfile(path):
        with open(path, "rb") as stream:
            document[name.replace("/", "_").replace(".json", "").replace(".jsonl", "") + "_sha256"] = hashlib.sha256(stream.read()).hexdigest()
with open(receipt + ".tmp", "w", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
os.chmod(receipt + ".tmp", 0o600)
os.replace(receipt + ".tmp", receipt)
PY
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
