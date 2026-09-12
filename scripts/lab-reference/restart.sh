#!/usr/bin/env bash
set -Eeuo pipefail

# Bounded no-load cold restart plus acknowledged-history re-audit of one
# retained WuKongIM point root (task 6.7 of the reference comparison).
#
# The coordinator selects the highest common point valid for both products
# and calls this once for its WuKongIM root. In order: verify the retained
# root and the frozen identities -> refuse any live product state -> render
# the identical Compose configuration -> start the same three containers on
# the retained data roots and time readiness -> re-read every acknowledged
# identity from history (no SEND) -> stop the containers boundedly ->
# inventory residual state -> add the restart section to the provenance ->
# re-emit the comparison document, keeping the pre-restart document.
# Restart time never enters any latency figure.

LAB_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=point.sh
source "$LAB_SCRIPT_DIR/point.sh"

readonly LAB_STOP_MODEL="bounded SIGTERM compose down after the point's drain, then a cold start of the same three containers on the retained data roots"

restart_usage() {
    cat <<'USAGE'
Usage: restart.sh --task-root DIR --run-root DIR --source-dir DIR --wkcli FILE --image NAME
                  --docker-context NAME --deadline-seconds N
                  [--node-memory 8g] [--container-user 0:0] [--allow-authorized-rootful]

Cold-restarts one retained WuKongIM point root once and re-audits its
acknowledged history; the pre-restart document is kept beside the new one.
USAGE
}

restart_main() {
    # Run state is global on purpose: the EXIT trap (finish) runs after this
    # frame is gone when errexit fires here, and it must still tear the Compose
    # project down and write the restart receipt.
    task_root="" run_root="" source_dir="" wkcli="" image="" docker_context="" deadline_seconds=""
    node_memory="8g" container_user="0:0" allow_rootful=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --task-root) task_root="$2"; shift 2 ;;
            --run-root) run_root="$2"; shift 2 ;;
            --source-dir) source_dir="$2"; shift 2 ;;
            --wkcli) wkcli="$2"; shift 2 ;;
            --image) image="$2"; shift 2 ;;
            --docker-context) docker_context="$2"; shift 2 ;;
            --deadline-seconds) deadline_seconds="$2"; shift 2 ;;
            --node-memory) node_memory="$2"; shift 2 ;;
            --container-user) container_user="$2"; shift 2 ;;
            --allow-authorized-rootful) allow_rootful=1; shift ;;
            -h|--help) restart_usage; return 0 ;;
            *) restart_usage >&2; die "unknown argument: $1" ;;
        esac
    done
    local name
    for name in task_root run_root source_dir wkcli image docker_context deadline_seconds; do
        [[ -n "${!name}" ]] || die "--${name//_/-} is required"
    done
    [[ "$deadline_seconds" =~ ^[0-9]+$ && "$deadline_seconds" -gt 0 ]] || die "--deadline-seconds must be a positive integer"
    for name in docker python3 curl ss sha256sum realpath taskset findmnt awk git cmp; do
        require_command "$name"
    done
    validate_task_root "$task_root"
    validate_retained_root "$task_root" "$run_root"
    below_task_root "$task_root" "$source_dir" || die "source dir must be below the task root"
    below_task_root "$task_root" "$wkcli" || die "wkcli must be below the task root"
    [[ -x "$wkcli" ]] || die "wkcli is not executable"
    compose_file="$source_dir/docker/lab/reference/compose.yml"
    conf_dir="$source_dir/docker/lab/reference/conf"
    [[ -f "$compose_file" ]] || die "laboratory Compose file is missing in the source dir"

    facts="$run_root/facts" restart_dir="$run_root/restart"
    rfacts="$restart_dir/facts" revidence="$restart_dir/evidence"
    sequence="" project="" instrumentation=""
    sequence="$(json_field "$facts/campaign.json" launch_sequence)"
    project="$(printf 'wk-ref-seq%02d' "$sequence")"
    if grep -q 'WK_METRICS_ENABLE: "true"' "$run_root/evidence/compose-config.yaml"; then
        instrumentation=on
    elif grep -q 'WK_METRICS_ENABLE: "false"' "$run_root/evidence/compose-config.yaml"; then
        instrumentation=off
    else
        die "the retained Compose configuration does not state the metrics switch"
    fi
    compose_vars=()
    mapfile -t compose_vars < <(compose_env "$run_root" "$conf_dir" "$image" "$project" "$instrumentation" "$node_memory" "$container_user")
    (( ${#compose_vars[@]} == 17 )) || die "compose environment is incomplete"

    phase="validate" cleanup_status="" reaudit_exit="" launched=0 created=0 started_utc=""
    started_utc="$(utc_now)"
    finish() {
        local status=$?
        trap - EXIT
        if [[ -z "$cleanup_status" && "$launched" == 1 ]]; then
            cleanup_status="$(compose_down_and_inventory "$docker_context" "$compose_file" "$project" "$run_root" "$revidence" "$rfacts" "$wkcli" "${compose_vars[@]}" || echo failed)"
        fi
        if [[ "$created" == 1 ]]; then
            python3 - "$restart_dir/restart-receipt.json" "$status" "$phase" "$sequence" "$run_root" "$reaudit_exit" "$cleanup_status" "$started_utc" "$(utc_now)" <<'PY'
import hashlib, json, os, sys
receipt, status, phase, sequence, run_root, reaudit_exit, cleanup_status, started, ended = sys.argv[1:]
document = {
    "schema": "wukongim-reference-restart-receipt/1",
    "product": "wukongim",
    "launch_sequence": int(sequence),
    "run_root": run_root,
    "result": "pass" if status == "0" else "fail",
    "exit_code": int(status),
    "failure_phase": None if status == "0" else phase,
    "reaudit_exit_code": int(reaudit_exit) if reaudit_exit else None,
    "cleanup_status": cleanup_status or "not_reached",
    "started_at_utc": started,
    "ended_at_utc": ended,
}
for name in ("comparison.json", "comparison.pre-restart.json", "restart/reaudit.json", "restart/restart.json"):
    path = os.path.join(run_root, name)
    if os.path.isfile(path):
        with open(path, "rb") as stream:
            document[name.replace("/", "_").replace(".json", "").replace(".pre-restart", "_pre_restart") + "_sha256"] = hashlib.sha256(stream.read()).hexdigest()
with open(receipt + ".tmp", "w", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
os.chmod(receipt + ".tmp", 0o600)
os.replace(receipt + ".tmp", receipt)
PY
        fi
        exit "$status"
    }
    trap finish EXIT

    phase="prepare"
    mkdir -p -- "$rfacts" "$revidence"
    created=1
    phase="containment"
    local -a containment=()
    mapfile -t containment < <(docker_containment "$docker_context" "$task_root" "$allow_rootful" "$rfacts/docker-info.raw.json")
    (( ${#containment[@]} == 5 )) || die "docker containment could not be proven"
    refuse_prior_state "$docker_context" "$wkcli"
    verify_frozen_identities "$source_dir" "$facts/source.json" "$facts/build.json" "$image" "$docker_context" "$wkcli"

    phase="compose-config"
    env "${compose_vars[@]}" docker --context "$docker_context" compose -f "$compose_file" config >"$revidence/compose-config.yaml"
    gate_compose_config "$revidence/compose-config.yaml" "$run_root" "$conf_dir"
    cmp -s "$revidence/compose-config.yaml" "$run_root/evidence/compose-config.yaml" || die "the restart would not use the point's exact Compose configuration"

    phase="compose-up"
    local stopped_at_utc started_at_utc started_ms ready_at_utc ready_ms restart_ms
    stopped_at_utc="$(cat "$run_root/evidence/stopped-at-utc")"
    log_event "cold-starting $project on its retained data roots"
    launched=1
    started_at_utc="$(utc_now)"
    started_ms="$(epoch_ms)"
    env "${compose_vars[@]}" docker --context "$docker_context" compose -f "$compose_file" up -d --no-build --no-recreate >"$revidence/compose-up.log" 2>&1
    phase="ready"
    wait_nodes_ready "$LAB_READY_TIMEOUT_SECONDS"
    ready_at_utc="$(utc_now)"
    ready_ms="$(epoch_ms)"
    restart_ms=$((ready_ms - started_ms))
    (( restart_ms > 0 )) || restart_ms=1
    inspect_project "$docker_context" "$project" "$rfacts"
    log_event "three nodes ready after ${restart_ms} ms"

    phase="reaudit"
    local -a servers=()
    local node
    for node in 1 2 3; do
        servers+=("http://127.0.0.1:$(api_port_for_node "$node")")
    done
    set +e
    timeout "$deadline_seconds" taskset -c "$LAB_CLIENT_CPUS" "$wkcli" bench reference reaudit \
        --server "${servers[0]}" --server "${servers[1]}" --server "${servers[2]}" \
        --ledger "$run_root/client/acknowledged-ledger.jsonl" --output "$restart_dir/reaudit.json" >"$revidence/reaudit.log" 2>&1
    reaudit_exit=$?
    set -e
    log_event "reaudit exit code $reaudit_exit"
    [[ -s "$restart_dir/reaudit.json" ]] || die "the re-audit wrote no result (exit $reaudit_exit)"

    phase="teardown"
    cleanup_status="$(compose_down_and_inventory "$docker_context" "$compose_file" "$project" "$run_root" "$revidence" "$rfacts" "$wkcli" "${compose_vars[@]}")"
    log_event "restart cleanup $cleanup_status"

    phase="restart-record"
    python3 - "$restart_dir/restart.json" "$restart_dir/reaudit.json" "$stopped_at_utc" "$started_at_utc" "$ready_at_utc" "$restart_ms" "$LAB_STOP_MODEL" <<'PY'
import json, os, sys
output, reaudit_path, stopped, started, ready, restart_ms, stop_model = sys.argv[1:]
reaudit = json.load(open(reaudit_path, encoding="utf-8"))
document = {
    "performed": True,
    "stop_model": stop_model,
    "stopped_at_utc": stopped,
    "started_at_utc": started,
    "ready_at_utc": ready,
    "restart_ms": int(restart_ms),
    "reaudit": reaudit["history"],
    "included_in_throughput_latency": False,
}
with open(output, "w", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
os.chmod(output, 0o600)
PY

    phase="provenance"
    local log_volume_bytes
    log_volume_bytes="$(cat "$facts/log-volume-bytes")"
    python3 "$LAB_SCRIPT_DIR/provenance.py" --campaign "$facts/campaign.json" --source "$facts/source.json" --build "$facts/build.json" \
        --host "$facts/host.json" --deployment "$facts/deployment.json" --docker-info "$facts/docker-info.json" \
        --docker-inspect "$facts/docker-inspect.json" --isolation "$facts/isolation.json" --resources "$facts/resources.json" \
        --cleanup "$facts/cleanup.json" --restart "$restart_dir/restart.json" \
        --node-config "$conf_dir/node1.toml" --node-config "$conf_dir/node2.toml" --node-config "$conf_dir/node3.toml" \
        --log-volume-bytes "$log_volume_bytes" --networks "$(paste -sd, "$facts/networks.txt" 2>/dev/null || true)" --volumes "" \
        --output "$restart_dir/provenance.json"

    phase="emit"
    [[ ! -e "$run_root/comparison.pre-restart.json" ]] || die "a pre-restart document already exists"
    cp -- "$run_root/comparison.json" "$run_root/comparison.pre-restart.json"
    "$wkcli" bench reference emit --run "$run_root/client/reference-run.json" --provenance "$restart_dir/provenance.json" --output "$restart_dir/comparison.json" >"$revidence/emit.log" 2>&1
    cp -- "$restart_dir/comparison.json" "$run_root/comparison.json"
    phase="done"
    [[ "$reaudit_exit" == 0 ]] || die "the re-audit exited $reaudit_exit; the restarted document records the incorrect history"
    [[ "$cleanup_status" == clean ]] || die "the restart cleanup left residual state; the next launch is blocked until an isolation proof"
    log_event "restart of sequence $sequence complete: $run_root/comparison.json"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    restart_main "$@"
fi
