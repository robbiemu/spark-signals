#!/usr/bin/env bash
set -euo pipefail

if test "$(id -u)" -ne 0; then
  printf 'Run this rotation as root (for example, with sudo).\n' >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=${1:-"$(dirname -- "$script_dir")"}
nats_env="$repo_root/deploy/runtime/nats.env"
runtime_bridge_env="$repo_root/deploy/runtime/bridge.env"
system_bridge_env=/etc/spark-signals/bridge.env
compose="$repo_root/deploy/nats/compose.yml"
backup_dir=$(mktemp -d)
changed=no
committed=no

cleanup() {
  local status=$?
  trap - EXIT
  if test "$status" -ne 0 && test "$changed" = yes && test "$committed" != yes; then
    cp -a "$backup_dir/nats.env" "$nats_env"
    cp -a "$backup_dir/runtime-bridge.env" "$runtime_bridge_env"
    cp -a "$backup_dir/system-bridge.env" "$system_bridge_env"
    docker compose -f "$compose" up -d --force-recreate >/dev/null 2>&1 || true
    systemctl restart spark-otel-bridge.service >/dev/null 2>&1 || true
    printf 'Rotation failed; previous files and services were restored.\n' >&2
  fi
  rm -rf -- "$backup_dir"
  exit "$status"
}
trap cleanup EXIT

require_secret_file() {
  local path=$1
  if test -L "$path" || ! test -f "$path"; then
    printf 'Required secret file is missing or is a symlink: %s\n' "$path" >&2
    exit 1
  fi
}

read_value() {
  local path=$1
  local key=$2
  local line
  ROTATED_VALUE=
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      "$key="*) ROTATED_VALUE=${line#*=} ;;
    esac
  done <"$path"
  if test -z "$ROTATED_VALUE"; then
    printf 'Required key is absent: %s\n' "$key" >&2
    exit 1
  fi
}

rewrite_value() {
  local path=$1
  local key=$2
  local value=$3
  local temporary line replacements=0
  temporary=$(mktemp "${path}.rotate.XXXXXX")
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      "$key="*)
        printf '%s=%s\n' "$key" "$value" >>"$temporary"
        replacements=$((replacements + 1))
        ;;
      *) printf '%s\n' "$line" >>"$temporary" ;;
    esac
  done <"$path"
  if test "$replacements" -ne 1; then
    rm -f -- "$temporary"
    printf 'Expected exactly one key while rotating: %s\n' "$key" >&2
    exit 1
  fi
  chown --reference="$path" "$temporary"
  chmod --reference="$path" "$temporary"
  mv -f -- "$temporary" "$path"
}

remove_otel_overrides() {
  local path=$1
  local temporary line
  temporary=$(mktemp "${path}.sanitize.XXXXXX")
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      OTEL_EXPORTER_OTLP_ENDPOINT=* | OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=* | \
        OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=* | OTEL_EXPORTER_OTLP_PROTOCOL=* | \
        OTEL_EXPORTER_OTLP_METRICS_PROTOCOL=* | OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=* | \
        OTEL_EXPORTER_OTLP_HEADERS=* | OTEL_EXPORTER_OTLP_METRICS_HEADERS=* | \
        OTEL_EXPORTER_OTLP_LOGS_HEADERS=*) ;;
      *) printf '%s\n' "$line" >>"$temporary" ;;
    esac
  done <"$path"
  chown --reference="$path" "$temporary"
  chmod --reference="$path" "$temporary"
  mv -f -- "$temporary" "$path"
}

for path in "$nats_env" "$runtime_bridge_env" "$system_bridge_env" "$compose"; do
  require_secret_file "$path"
done

cp -a "$nats_env" "$backup_dir/nats.env"
cp -a "$runtime_bridge_env" "$backup_dir/runtime-bridge.env"
cp -a "$system_bridge_env" "$backup_dir/system-bridge.env"

read_value "$nats_env" SPARK_BRIDGE_PASSWORD
old_password=$ROTATED_VALUE
new_password=$(openssl rand -hex 32)
if test -z "$new_password" || test "$new_password" = "$old_password"; then
  printf 'Password generation failed.\n' >&2
  exit 1
fi

rewrite_value "$nats_env" SPARK_BRIDGE_PASSWORD "$new_password"
rewrite_value "$runtime_bridge_env" NATS_PASSWORD "$new_password"
rewrite_value "$system_bridge_env" NATS_PASSWORD "$new_password"
remove_otel_overrides "$runtime_bridge_env"
remove_otel_overrides "$system_bridge_env"
changed=yes

docker compose -f "$compose" up -d --force-recreate >/dev/null
systemctl restart spark-otel-bridge.service
sleep 5
systemctl is-active --quiet spark-agent.service
systemctl is-active --quiet spark-otel-bridge.service

python3 - "$backup_dir/runtime-bridge.env" "$system_bridge_env" <<'PY'
import json
import socket
import sys


def password(path):
    with open(path, encoding="utf-8") as source:
        for line in source:
            if line.startswith("NATS_PASSWORD="):
                return line.rstrip("\n").split("=", 1)[1]
    raise RuntimeError("NATS_PASSWORD missing")


def authenticates(secret):
    with socket.create_connection(("127.0.0.1", 4222), timeout=3) as connection:
        connection.recv(8192)
        payload = json.dumps(
            {"user": "spark-bridge", "pass": secret, "verbose": True}
        ).encode()
        connection.sendall(b"CONNECT " + payload + b"\r\nPING\r\n")
        response = b""
        while b"PONG" not in response and b"-ERR" not in response:
            chunk = connection.recv(8192)
            if not chunk:
                break
            response += chunk
        return b"PONG" in response and b"-ERR" not in response


old_works = authenticates(password(sys.argv[1]))
new_works = authenticates(password(sys.argv[2]))
if old_works or not new_works:
    raise SystemExit(1)
PY

unset old_password new_password ROTATED_VALUE
committed=yes
printf 'spark-bridge NATS password rotated; new credential accepted and old credential rejected\n'
printf 'agent and bridge remained active; OTLP environment overrides removed\n'
