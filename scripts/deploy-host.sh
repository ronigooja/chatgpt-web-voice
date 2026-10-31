#!/usr/bin/env bash
# Host operator script for the relay deployment.
# Prerequisites: run as root with Docker Compose, gh logged in, sha256sum, and
# python3. Provision gh without exposing a token: on a trusted workstation use
# `gh auth token | ssh root@HOST 'umask 077; mkdir -p /root/.config/gh; cat > /root/.config/gh/hosts.yml; chmod 600 /root/.config/gh/hosts.yml'`.
# The cloud security group must allow the configured UDP range. Behind NAT,
# pass the host's public IPv4 with --public-ip. Updates restart the service and
# interrupt live calls.
set -euo pipefail

readonly REPO="ronigooja/chatgpt-web-voice"
readonly DEFAULT_DIR="/opt/llm-auth-bridge"
readonly CONTAINER="llm-auth-bridge-voice"
readonly DEFAULT_UDP_MIN=40000
readonly DEFAULT_UDP_MAX=40099
TMP_DIR=""

usage() {
  cat <<'EOF'
Usage:
  deploy-host.sh update --run-id N [--compose-dir DIR] [--public-ip IP]
                       [--udp-min PORT] [--udp-max PORT] [--dry-run]
  deploy-host.sh rollback [--compose-dir DIR]
  deploy-host.sh status [--compose-dir DIR]
  deploy-host.sh uninstall [--compose-dir DIR] [--purge-data]
  deploy-host.sh --help

Prerequisites: run as root with Docker Compose, gh installed and logged in,
sha256sum, and python3. Provision gh from a trusted workstation without
showing the token:
  gh auth token | ssh root@HOST 'umask 077; mkdir -p /root/.config/gh; cat > /root/.config/gh/hosts.yml; chmod 600 /root/.config/gh/hosts.yml'

The update verifies the GitHub Actions artifact checksum and source revision,
keeps compose.yaml.prev, validates Compose, waits for a healthy container, and
restores the previous compose file on a failed health check. The relay UDP
range must also be allowed by the cloud security group. Behind NAT, pass the
host public IPv4 with --public-ip (otherwise ifconfig.me is queried). Any
restart interrupts live calls. --dry-run prints the planned compose diff only.
EOF
}

die() { echo "deploy-host.sh: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }
valid_port() { [[ "$1" =~ ^[0-9]+$ ]] && (( 10#$1 >= 1 && 10#$1 <= 65535 )); }
valid_ipv4() {
  local part
  local -a parts
  IFS=. read -r -a parts <<<"$1"
  (( ${#parts[@]} == 4 )) || return 1
  for part in "${parts[@]}"; do
    [[ "$part" =~ ^[0-9]{1,3}$ ]] && (( 10#$part <= 255 )) || return 1
  done
}
cleanup() { [[ -z "$TMP_DIR" || ! -d "$TMP_DIR" ]] || rm -rf -- "$TMP_DIR"; }
trap cleanup EXIT

[[ $EUID -eq 0 ]] || die "must run as root"
COMMAND="${1:-}"
if [[ "$COMMAND" == "--help" || "$COMMAND" == "-h" ]]; then usage; exit 0; fi
[[ -n "$COMMAND" ]] || { usage >&2; exit 1; }
shift

COMPOSE_DIR="$DEFAULT_DIR"
RUN_ID=""
PUBLIC_IP=""
UDP_MIN=$DEFAULT_UDP_MIN
UDP_MAX=$DEFAULT_UDP_MAX
DRY_RUN=false
PURGE_DATA=false

while (($#)); do
  case "$1" in
    --compose-dir) (($# >= 2)) || die "$1 requires a value"; COMPOSE_DIR=$2; shift 2 ;;
    --run-id) (($# >= 2)) || die "$1 requires a value"; RUN_ID=$2; shift 2 ;;
    --public-ip) (($# >= 2)) || die "$1 requires a value"; PUBLIC_IP=$2; shift 2 ;;
    --udp-min) (($# >= 2)) || die "$1 requires a value"; UDP_MIN=$2; shift 2 ;;
    --udp-max) (($# >= 2)) || die "$1 requires a value"; UDP_MAX=$2; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --purge-data) PURGE_DATA=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown argument: $1 (use --help)" ;;
  esac
done

COMPOSE_FILE="$COMPOSE_DIR/compose.yaml"
PREVIOUS_FILE="$COMPOSE_DIR/compose.yaml.prev"
compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

case "$COMMAND" in
  update)
    [[ "$RUN_ID" =~ ^[0-9]+$ ]] || die "update requires --run-id N"
    valid_port "$UDP_MIN" || die "invalid UDP minimum port: $UDP_MIN"
    valid_port "$UDP_MAX" || die "invalid UDP maximum port: $UDP_MAX"
    (( 10#$UDP_MIN <= 10#$UDP_MAX )) || die "UDP minimum exceeds maximum"
    [[ -f "$COMPOSE_FILE" ]] || die "missing $COMPOSE_FILE"
    need docker; need gh; need sha256sum; need python3
    if [[ -z "$PUBLIC_IP" ]]; then
      need curl
      PUBLIC_IP=$(curl -4 -fsS https://ifconfig.me) || die "could not autodetect public IPv4; pass --public-ip"
      echo "Using autodetected public IPv4: $PUBLIC_IP"
    else
      echo "Using public IPv4: $PUBLIC_IP"
    fi
    valid_ipv4 "$PUBLIC_IP" || die "invalid public IPv4: $PUBLIC_IP"
    TMP_DIR=$(mktemp -d)
    HEAD_SHA=$(gh api "repos/$REPO/actions/runs/$RUN_ID" --jq .head_sha)
    [[ "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]] || die "invalid run head SHA"
    ARTIFACT_DIR="$TMP_DIR/artifact"
    mkdir -p "$ARTIFACT_DIR"
    gh run download "$RUN_ID" -R "$REPO" -n "voice-image-$HEAD_SHA" -D "$ARTIFACT_DIR"
    for artifact_file in SHA256SUMS SOURCE_REVISION IMAGE_TAG voice-image.tar.gz; do
      [[ -f "$ARTIFACT_DIR/$artifact_file" ]] || die "artifact missing $artifact_file"
    done
    (cd "$ARTIFACT_DIR" && sha256sum -c SHA256SUMS)
    SOURCE_REVISION=$(<"$ARTIFACT_DIR/SOURCE_REVISION")
    [[ "$SOURCE_REVISION" == "$HEAD_SHA" ]] || die "SOURCE_REVISION does not match run head SHA"
    IMAGE_TAG=$(<"$ARTIFACT_DIR/IMAGE_TAG")
    [[ "$IMAGE_TAG" =~ ^[A-Za-z0-9._/-]+:[A-Za-z0-9._-]+$ ]] || die "invalid IMAGE_TAG"
    python_args=("$COMPOSE_FILE" "$IMAGE_TAG" "$PUBLIC_IP" "$UDP_MIN" "$UDP_MAX")
    if [[ "$DRY_RUN" == true ]]; then
      python3 - "${python_args[@]}" <<'PY'
import difflib
import pathlib
import sys

path, image, public_ip, udp_min, udp_max = sys.argv[1:]
old = pathlib.Path(path).read_text()
lines = old.splitlines(keepends=True)
service = next((i for i, line in enumerate(lines) if line.rstrip("\r\n") == "  voice:"), None)
if service is None:
    raise SystemExit("compose.yaml has no voice service")
end = next((i for i in range(service + 1, len(lines)) if lines[i].startswith("  ") and not lines[i].startswith("    ")), len(lines))
block = lines[service:end]
def key(name):
    return next((i for i, line in enumerate(block) if line.startswith("    " + name + ":")), None)
def section_end(index):
    return next((i for i in range(index + 1, len(block)) if block[i].startswith("    ") and not block[i].startswith("      ")), len(block))
image_i = key("image")
if image_i is None:
    raise SystemExit("voice service has no image key")
block[image_i] = f'    image: "{image}"\n'
env_i = key("environment")
if env_i is None:
    block.append("    environment:\n")
    env_i = len(block) - 1
env_end = section_end(env_i)
values = {"VOICE_RELAY_ENABLED": '"true"', "VOICE_RELAY_PUBLIC_IP": f'"{public_ip}"', "VOICE_RELAY_UDP_PORT_MIN": f'"{udp_min}"', "VOICE_RELAY_UDP_PORT_MAX": f'"{udp_max}"'}
for name, value in values.items():
    found = next((i for i in range(env_i + 1, env_end) if block[i].startswith("      " + name + ":")), None)
    if found is None:
        block.insert(env_end, f"      {name}: {value}\n")
        env_end += 1
    else:
        block[found] = f"      {name}: {value}\n"
ports_i = key("ports")
if ports_i is None:
    block.append("    ports:\n")
    ports_i = len(block) - 1
ports_end = section_end(ports_i)
udp_line = f'      "{udp_min}-{udp_max}:{udp_min}-{udp_max}/udp"\n'
if not any(line.strip().strip('"') == udp_line.strip().strip('"') for line in block[ports_i + 1:ports_end]):
    block.insert(ports_end, udp_line)
new = "".join(lines[:service] + block + lines[end:])
sys.stdout.writelines(difflib.unified_diff(old.splitlines(True), new.splitlines(True), fromfile=path, tofile=path + " (planned)"))
PY
      exit 0
    fi
    if ! grep -Fq "image: \"$IMAGE_TAG\"" "$COMPOSE_FILE" || ! grep -Fq "\"$UDP_MIN-$UDP_MAX:$UDP_MIN-$UDP_MAX/udp\"" "$COMPOSE_FILE" || ! grep -Fq 'VOICE_RELAY_ENABLED: "true"' "$COMPOSE_FILE" || ! grep -Fq "VOICE_RELAY_PUBLIC_IP: \"$PUBLIC_IP\"" "$COMPOSE_FILE" || ! grep -Fq "VOICE_RELAY_UDP_PORT_MIN: \"$UDP_MIN\"" "$COMPOSE_FILE" || ! grep -Fq "VOICE_RELAY_UDP_PORT_MAX: \"$UDP_MAX\"" "$COMPOSE_FILE"; then
      cp -p "$COMPOSE_FILE" "$PREVIOUS_FILE"
      python3 - "${python_args[@]}" <<'PY'
import pathlib
import sys

path, image, public_ip, udp_min, udp_max = sys.argv[1:]
p = pathlib.Path(path)
lines = p.read_text().splitlines(keepends=True)
service = next((i for i, line in enumerate(lines) if line.rstrip("\r\n") == "  voice:"), None)
if service is None:
    raise SystemExit("compose.yaml has no voice service")
end = next((i for i in range(service + 1, len(lines)) if lines[i].startswith("  ") and not lines[i].startswith("    ")), len(lines))
block = lines[service:end]
def key(name):
    return next((i for i, line in enumerate(block) if line.startswith("    " + name + ":")), None)
def section_end(index):
    return next((i for i in range(index + 1, len(block)) if block[i].startswith("    ") and not block[i].startswith("      ")), len(block))
image_i = key("image")
if image_i is None:
    raise SystemExit("voice service has no image key")
block[image_i] = f'    image: "{image}"\n'
env_i = key("environment")
if env_i is None:
    block.append("    environment:\n")
    env_i = len(block) - 1
env_end = section_end(env_i)
values = {"VOICE_RELAY_ENABLED": '"true"', "VOICE_RELAY_PUBLIC_IP": f'"{public_ip}"', "VOICE_RELAY_UDP_PORT_MIN": f'"{udp_min}"', "VOICE_RELAY_UDP_PORT_MAX": f'"{udp_max}"'}
for name, value in values.items():
    found = next((i for i in range(env_i + 1, env_end) if block[i].startswith("      " + name + ":")), None)
    if found is None:
        block.insert(env_end, f"      {name}: {value}\n")
        env_end += 1
    else:
        block[found] = f"      {name}: {value}\n"
ports_i = key("ports")
if ports_i is None:
    block.append("    ports:\n")
    ports_i = len(block) - 1
ports_end = section_end(ports_i)
udp_line = f'      "{udp_min}-{udp_max}:{udp_min}-{udp_max}/udp"\n'
if not any(line.strip().strip('"') == udp_line.strip().strip('"') for line in block[ports_i + 1:ports_end]):
    block.insert(ports_end, udp_line)
p.write_text("".join(lines[:service] + block + lines[end:]))
PY
    fi
    compose config -q
    (cd "$ARTIFACT_DIR" && gunzip -c voice-image.tar.gz | docker load)
    compose up -d
    healthy=false
    for _ in {1..60}; do
      if [[ "$(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || true)" == healthy ]]; then healthy=true; break; fi
      sleep 1
    done
    if [[ "$healthy" != true ]]; then
      echo "container did not become healthy; restoring compose.yaml.prev" >&2
      cp -p "$PREVIOUS_FILE" "$COMPOSE_FILE"
      compose config -q
      compose up -d
      die "health check failed; previous compose configuration restored"
    fi
    if compose logs --no-color voice 2>/dev/null | grep -Fq media_relay_enabled; then
      echo "media_relay_enabled log line found"
    else
      echo "warning: media_relay_enabled log line not found" >&2
    fi
    echo "Published UDP ports:"
    docker port "$CONTAINER" 40000-40099/udp 2>/dev/null || true
    echo "Reminder: allow UDP $UDP_MIN-$UDP_MAX in the cloud security group."
    ;;
  rollback)
    [[ -f "$PREVIOUS_FILE" ]] || die "missing $PREVIOUS_FILE"
    need docker
    cp -p "$PREVIOUS_FILE" "$COMPOSE_FILE"
    compose config -q
    compose up -d
    echo "Rollback complete; restarting interrupts live calls."
    ;;
  status)
    need docker
    compose ps
    echo "Health: $(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || echo unavailable)"
    echo "Published UDP ports:"
    docker port "$CONTAINER" 40000-40099/udp 2>/dev/null || true
    ;;
  uninstall)
    need docker
    compose down
    if [[ "$PURGE_DATA" == true ]]; then
      compose down -v
      rm -rf -- "$COMPOSE_DIR/.secrets"
      echo "Removed service, named volumes, and .secrets."
    else
      echo "Removed service; named volumes and .secrets were kept."
    fi
    ;;
  *) die "unknown subcommand: $COMMAND (use --help)" ;;
esac
