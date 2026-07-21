#!/usr/bin/env bash
set -euo pipefail

if test "$(id -u)" -ne 0; then
  printf 'Run this installer as root (for example, with sudo).\n' >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=${1:-"$(dirname -- "$script_dir")"}
legacy_user=${2:-}
config_dir=/etc/spark-signals
capability_source_dir="$repo_root/deploy/hardware-capabilities"
capability_config_dir="$config_dir/hardware-capabilities"
signal_policy_source_dir="$repo_root/deploy/signal-policies"
signal_policy_config_dir="$config_dir/signal-policies"
state_dir=/var/lib/spark-signals
maple_credential=/etc/srvmini2/spark-signals/maple-otlp-client.json

require_regular_file() {
  local path=$1
  if test -L "$path" || ! test -f "$path"; then
    printf 'Required regular file is missing or is a symlink: %s\n' "$path" >&2
    exit 1
  fi
}

create_service_user() {
  local account=$1
  if ! getent group "$account" >/dev/null; then
    groupadd --system "$account"
  fi
  if ! getent passwd "$account" >/dev/null; then
    useradd --system --gid "$account" --home-dir /nonexistent \
      --shell /usr/sbin/nologin "$account"
  fi
}

if test -L "$capability_source_dir" || ! test -d "$capability_source_dir"; then
  printf 'Hardware capability profile directory is missing or is a symlink: %s\n' \
    "$capability_source_dir" >&2
  exit 1
fi
shopt -s nullglob
capability_profiles=("$capability_source_dir"/*.toml)
shopt -u nullglob
if test "${#capability_profiles[@]}" -eq 0; then
  printf 'No hardware capability profiles were found in: %s\n' \
    "$capability_source_dir" >&2
  exit 1
fi
for profile in "${capability_profiles[@]}"; do
  require_regular_file "$profile"
done

if test -L "$signal_policy_source_dir" || ! test -d "$signal_policy_source_dir"; then
  printf 'Signal policy directory is missing or is a symlink: %s\n' \
    "$signal_policy_source_dir" >&2
  exit 1
fi
shopt -s nullglob
signal_policy_files=("$signal_policy_source_dir"/*.toml)
shopt -u nullglob
if test "${#signal_policy_files[@]}" -eq 0; then
  printf 'No signal policy files were found in: %s\n' \
    "$signal_policy_source_dir" >&2
  exit 1
fi
for policy in "${signal_policy_files[@]}"; do
  require_regular_file "$policy"
done

user_systemctl() {
  local account=$1
  shift
  local uid
  uid=$(id -u "$account")
  if ! test -S "/run/user/$uid/bus"; then
    return 1
  fi
  runuser -u "$account" -- env \
    XDG_RUNTIME_DIR="/run/user/$uid" \
    DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$uid/bus" \
    systemctl --user "$@"
}

restore_legacy_agent() {
  if test "$legacy_agent_enabled" = yes; then
    user_systemctl "$legacy_user" enable spark-agent.service >/dev/null 2>&1 || true
  fi
  if test "$legacy_agent_active" = yes; then
    user_systemctl "$legacy_user" start spark-agent.service >/dev/null 2>&1 || true
  fi
}

for path in \
  "$repo_root/target/release/spark-agent" \
  "$repo_root/target/release/spark-otel-bridge" \
  "$repo_root/deploy/example-config/agent.toml" \
  "$repo_root/deploy/systemd/spark-agent.service" \
  "$repo_root/deploy/systemd/spark-otel-bridge.service" \
  "$repo_root/deploy/runtime/agent.env"; do
  require_regular_file "$path"
done

if test -e "$repo_root/deploy/runtime/bridge.env"; then
  require_regular_file "$repo_root/deploy/runtime/bridge.env"
  if grep -Eq '^OTEL_EXPORTER_OTLP(_(METRICS|LOGS))?_(ENDPOINT|PROTOCOL|HEADERS)=' \
    "$repo_root/deploy/runtime/bridge.env"; then
    printf 'Refusing to install Maple endpoint, protocol, or authorization overrides from an environment file.\n' >&2
    exit 1
  fi
fi

if test -n "$legacy_user"; then
  if ! id "$legacy_user" >/dev/null 2>&1; then
    printf 'Legacy service user does not exist: %s\n' "$legacy_user" >&2
    exit 1
  fi
  legacy_uid=$(id -u "$legacy_user")
  if ! test -S "/run/user/$legacy_uid/bus"; then
    printf 'The legacy user manager is unavailable; log in as %s and disable its units first.\n' \
      "$legacy_user" >&2
    exit 1
  fi
fi

create_service_user spark-signals-agent
create_service_user spark-signals-bridge
install -d -o spark-signals-agent -g spark-signals-agent -m 0750 "$state_dir"
for device_group in video render; do
  if getent group "$device_group" >/dev/null; then
    usermod --append --groups "$device_group" spark-signals-agent
  fi
done

install -d -o root -g root -m 0755 "$config_dir"
install -d -o root -g root -m 0755 "$capability_config_dir"
find "$capability_config_dir" -mindepth 1 -maxdepth 1 -type f -name '*.toml' -delete
install -o root -g root -m 0644 \
  "${capability_profiles[@]}" "$capability_config_dir/"
install -d -o root -g root -m 0755 "$signal_policy_config_dir"
find "$signal_policy_config_dir" -mindepth 1 -maxdepth 1 -type f -name '*.toml' -delete
install -o root -g root -m 0644 \
  "${signal_policy_files[@]}" "$signal_policy_config_dir/"
install -o root -g root -m 0755 \
  "$repo_root/target/release/spark-agent" /usr/local/bin/spark-agent
install -o root -g root -m 0755 \
  "$repo_root/target/release/spark-otel-bridge" /usr/local/bin/spark-otel-bridge
install -o root -g root -m 0644 \
  "$repo_root/deploy/example-config/agent.toml" "$config_dir/agent.toml"
install -o root -g spark-signals-agent -m 0640 \
  "$repo_root/deploy/runtime/agent.env" "$config_dir/agent.env"
if test -f "$repo_root/deploy/runtime/bridge.env"; then
  install -o root -g spark-signals-bridge -m 0640 \
    "$repo_root/deploy/runtime/bridge.env" "$config_dir/bridge.env"
fi
install -o root -g root -m 0644 \
  "$repo_root/deploy/systemd/spark-agent.service" \
  /etc/systemd/system/spark-agent.service
install -o root -g root -m 0644 \
  "$repo_root/deploy/systemd/spark-otel-bridge.service" \
  /etc/systemd/system/spark-otel-bridge.service

systemctl daemon-reload

legacy_agent_active=no
legacy_agent_enabled=no
if test -n "$legacy_user"; then
  if user_systemctl "$legacy_user" is-active --quiet spark-agent.service; then
    legacy_agent_active=yes
  fi
  if user_systemctl "$legacy_user" is-enabled --quiet spark-agent.service; then
    legacy_agent_enabled=yes
  fi
  user_systemctl "$legacy_user" disable --now spark-agent.service >/dev/null 2>&1 || true
  user_systemctl "$legacy_user" disable --now spark-otel-bridge.service >/dev/null 2>&1 || true
fi

if ! systemctl enable spark-agent.service || ! systemctl restart spark-agent.service; then
  restore_legacy_agent
  printf 'System agent failed to start; legacy user service state was restored.\n' >&2
  exit 1
fi

sleep 3
if ! systemctl is-active --quiet spark-agent.service; then
  systemctl disable --now spark-agent.service >/dev/null 2>&1 || true
  restore_legacy_agent
  printf 'System agent did not remain active; legacy user service state was restored.\n' >&2
  exit 1
fi
printf 'spark-agent: active system service as spark-signals-agent\n'
if test -e "$maple_credential"; then
  require_regular_file "$maple_credential"
  credential_owner=$(stat -c %u "$maple_credential")
  credential_mode=$(stat -c %a "$maple_credential")
  if test "$credential_owner" != 0 || test "$credential_mode" != 600; then
    printf 'Maple credential must be root-owned with mode 0600.\n' >&2
    exit 1
  fi
  if ! getent hosts srvmini2.lan >/dev/null; then
    printf 'srvmini2.lan does not resolve on this host.\n' >&2
    exit 1
  fi
  if ! systemctl enable spark-otel-bridge.service || ! systemctl restart spark-otel-bridge.service; then
    systemctl disable --now spark-otel-bridge.service >/dev/null 2>&1 || true
    printf 'spark-otel-bridge failed to start with the Maple credential.\n' >&2
    exit 1
  fi
  sleep 3
  if ! systemctl is-active --quiet spark-otel-bridge.service; then
    systemctl disable --now spark-otel-bridge.service >/dev/null 2>&1 || true
    printf 'spark-otel-bridge did not remain active.\n' >&2
    exit 1
  fi
  printf 'spark-otel-bridge: active system service with Maple credential\n'
else
  systemctl disable --now spark-otel-bridge.service >/dev/null 2>&1 || true
  printf 'spark-otel-bridge: installed but disabled; Maple credential is absent\n'
fi
if test -n "$legacy_user"; then
  printf 'legacy user units disabled for: %s\n' "$legacy_user"
fi
