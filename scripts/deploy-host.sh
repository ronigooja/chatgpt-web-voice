#!/usr/bin/env bash
# Host operator script for the relay deployment.
# Prerequisites for install: root, Docker with its Compose plugin, gh logged in,
# and apt-get on Debian/Ubuntu when gh is missing. The cloud security group
# must allow the UDP range; behind NAT pass --public-ip. Install prints the
# generated admin username and password once at the end; the token encryption
# key is never printed. Updates restart the service and interrupt live calls.
# Provision gh without exposing a token:
# gh auth token | ssh root@HOST 'umask 077; mkdir -p /root/.config/gh; cat > /root/.config/gh/hosts.yml; chmod 600 /root/.config/gh/hosts.yml'
set -euo pipefail

readonly REPO="ronigooja/chatgpt-web-voice"
readonly DEFAULT_DIR="/opt/llm-auth-bridge"
readonly CONTAINER="llm-auth-bridge-voice"
readonly DEFAULT_UDP_MIN=40000
readonly DEFAULT_UDP_MAX=40099
TMP_DIR=""
INSTALL_MODE=false

usage() {
  cat <<'USAGE'
Usage:
  deploy-host.sh install --run-id N [--compose-dir DIR] [--public-ip IP]
                         [--udp-min PORT] [--udp-max PORT]
                         [--admin-user NAME] [--dry-run]
  deploy-host.sh update --run-id N [--compose-dir DIR] [--public-ip IP]
                        [--udp-min PORT] [--udp-max PORT] [--dry-run]
  deploy-host.sh rollback [--compose-dir DIR]
  deploy-host.sh status [--compose-dir DIR]
  deploy-host.sh uninstall [--compose-dir DIR] [--purge-data]
  deploy-host.sh --help

Prerequisites: install requires root, Docker with its Compose plugin, and gh
logged in. If gh is missing, install uses apt-get only on Debian/Ubuntu. From
a trusted workstation, provision gh without showing the token:
  gh auth token | ssh root@HOST 'umask 077; mkdir -p /root/.config/gh; cat > /root/.config/gh/hosts.yml; chmod 600 /root/.config/gh/hosts.yml'

The cloud security group must allow the configured UDP range. Behind NAT, pass
the host public IPv4 with --public-ip (otherwise update queries ifconfig.me).
Install creates .env mode 600 and prints the generated admin username and
password once at the end; the password is also stored there. The token
encryption key is never printed. Updates verify the GitHub Actions artifact
checksum and source revision, keep compose.yaml.prev, validate Compose, wait
for a healthy container, and restore the previous compose file on failure.
Any restart interrupts live calls. --dry-run prints planned content without
creating files or downloading artifacts.
USAGE
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
compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

ensure_gh_for_install() {
  if ! command -v gh >/dev/null 2>&1; then
    need apt-get
    [[ -r /etc/os-release ]] || die "cannot determine OS; install gh manually on Debian/Ubuntu"
    # shellcheck disable=SC1091
    . /etc/os-release
    case "${ID:-}" in
      debian|ubuntu) ;;
      *) die "gh is missing; automatic installation is supported only on Debian/Ubuntu" ;;
    esac
    echo "gh not found; installing it with apt-get."
    apt-get update
    apt-get install -y gh
  fi
  gh auth status >/dev/null 2>&1 || die "gh is not authenticated; provision it with the command in deploy-host.sh --help"
}
check_install_prereqs() {
  need docker
  docker compose version >/dev/null 2>&1 || die "Docker Compose plugin is required (install docker compose plugin)"
  ensure_gh_for_install
  need sha256sum
  need python3
  need openssl
}
resolve_public_ip() {
  if [[ -z "$PUBLIC_IP" ]]; then
    need curl
    PUBLIC_IP=$(curl -4 -fsS https://ifconfig.me) || die "could not autodetect public IPv4; pass --public-ip"
    echo "Using autodetected public IPv4: $PUBLIC_IP"
  else
    echo "Using public IPv4: $PUBLIC_IP"
  fi
  valid_ipv4 "$PUBLIC_IP" || die "invalid public IPv4: $PUBLIC_IP"
}
prepare_artifact() {
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
}
render_compose() {
  cat <<EOF_COMPOSE
services:
  voice:
    image: "$1"
    container_name: $CONTAINER
    restart: unless-stopped
    ports:
      - "127.0.0.1:21387:8090"
      - "$4-$5:$4-$5/udp"
    environment:
      VOICE_AUTH_USERNAME: \${VOICE_AUTH_USERNAME}
      VOICE_AUTH_PASSWORD: \${VOICE_AUTH_PASSWORD}
      VOICE_TOKEN_ENCRYPTION_KEY: \${VOICE_TOKEN_ENCRYPTION_KEY}
      VOICE_TLS: "false"
      VOICE_LISTEN_ADDR: 0.0.0.0:8090
      VOICE_ENV: production
      VOICE_DATABASE_FILE: /app/data/voice.db
      VOICE_RELAY_ENABLED: "true"
      VOICE_RELAY_PUBLIC_IP: "$2"
      VOICE_RELAY_UDP_PORT_MIN: "$4"
      VOICE_RELAY_UDP_PORT_MAX: "$5"
    volumes:
      - voice-data:/app/data
    healthcheck:
      test: ["CMD", "wget", "-q", "-T", "3", "-O", "/dev/null", "http://127.0.0.1:8090/login"]
      interval: 30s
      timeout: 5s
      retries: 5
volumes:
  voice-data:
EOF_COMPOSE
}
write_install_env() {
  umask 077
  : >"$ENV_FILE"
  chmod 600 "$ENV_FILE"
  {
    printf 'VOICE_AUTH_USERNAME=%s\n' "$ADMIN_USER"
    printf 'VOICE_AUTH_PASSWORD='
    cat "$TMP_DIR/admin-password"
    printf '\nVOICE_TOKEN_ENCRYPTION_KEY='
    cat "$TMP_DIR/token-key"
    printf '\n'
  } >"$ENV_FILE"
}
generate_install_secrets() {
  umask 077
  openssl rand -base64 128 | tr -dc 'A-Za-z0-9_-' >"$TMP_DIR/admin-password.raw"
  [[ $(wc -c <"$TMP_DIR/admin-password.raw") -ge 48 ]] || die "failed to generate admin password"
  dd if="$TMP_DIR/admin-password.raw" of="$TMP_DIR/admin-password" bs=1 count=48 status=none
  openssl rand -hex 32 >"$TMP_DIR/token-key"
  write_install_env
}

update_compose() {
  python3 - "$COMPOSE_FILE" "$IMAGE_TAG" "$PUBLIC_IP" "$UDP_MIN" "$UDP_MAX" "$1" <<'PY'
import difflib
import pathlib
import sys
path, image, public_ip, udp_min, udp_max, dry_run = sys.argv[1:]
p = pathlib.Path(path)
old = p.read_text()
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
udp_line = f'      - "{udp_min}-{udp_max}:{udp_min}-{udp_max}/udp"\n'
udp_spec = f'{udp_min}-{udp_max}:{udp_min}-{udp_max}/udp'
if not any(line.strip().lstrip('-').strip().strip('"').strip("'") == udp_spec for line in block[ports_i + 1:ports_end]):
    block.insert(ports_end, udp_line)
new = "".join(lines[:service] + block + lines[end:])
if dry_run == "true":
    sys.stdout.writelines(difflib.unified_diff(old.splitlines(True), new.splitlines(True), fromfile=str(path), tofile=str(path) + " (planned)"))
else:
    p.write_text(new)
PY
}
wait_for_health() {
  local healthy=false
  for _ in {1..60}; do
    if [[ "$(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || true)" == healthy ]]; then healthy=true; break; fi
    sleep 1
  done
  [[ "$healthy" == true ]]
}
deploy_artifact() {
  compose config -q
  (cd "$ARTIFACT_DIR" && gunzip -c voice-image.tar.gz | docker load)
  compose up -d
  if ! wait_for_health; then
    if [[ "$INSTALL_MODE" == true ]]; then
      echo "container did not become healthy; inspect with: docker compose -f $COMPOSE_FILE logs voice" >&2
      die "health check failed; generated .env was kept at $ENV_FILE"
    fi
    echo "container did not become healthy; restoring compose.yaml.prev" >&2
    cp -p "$PREVIOUS_FILE" "$COMPOSE_FILE"
    compose config -q
    compose up -d
    die "health check failed; previous compose configuration restored"
  fi
  if compose logs --no-color voice 2>/dev/null | grep -Fq media_relay_enabled; then echo "media_relay_enabled log line found"; else echo "warning: media_relay_enabled log line not found" >&2; fi
  echo "Published UDP ports:"
  docker port "$CONTAINER" 2>/dev/null | grep "/udp" || true
  echo "Reminder: allow UDP $UDP_MIN-$UDP_MAX in the cloud security group."
}

COMMAND="${1:-}"
if [[ "$COMMAND" == "--help" || "$COMMAND" == "-h" ]]; then usage; exit 0; fi
[[ -n "$COMMAND" ]] || { usage >&2; exit 1; }
[[ $EUID -eq 0 ]] || die "must run as root"
shift
COMPOSE_DIR="$DEFAULT_DIR"
RUN_ID=""
PUBLIC_IP=""
UDP_MIN=$DEFAULT_UDP_MIN
UDP_MAX=$DEFAULT_UDP_MAX
ADMIN_USER='admin'
DRY_RUN=false
PURGE_DATA=false
while (($#)); do
  case "$1" in
    --compose-dir) (($# >= 2)) || die "$1 requires a value"; COMPOSE_DIR=$2; shift 2 ;;
    --run-id) (($# >= 2)) || die "$1 requires a value"; RUN_ID=$2; shift 2 ;;
    --public-ip) (($# >= 2)) || die "$1 requires a value"; PUBLIC_IP=$2; shift 2 ;;
    --udp-min) (($# >= 2)) || die "$1 requires a value"; UDP_MIN=$2; shift 2 ;;
    --udp-max) (($# >= 2)) || die "$1 requires a value"; UDP_MAX=$2; shift 2 ;;
    --admin-user) (($# >= 2)) || die "$1 requires a value"; ADMIN_USER=$2; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --purge-data) PURGE_DATA=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown argument: $1 (use --help)" ;;
  esac
done
COMPOSE_FILE="$COMPOSE_DIR/compose.yaml"
PREVIOUS_FILE="$COMPOSE_DIR/compose.yaml.prev"
ENV_FILE="$COMPOSE_DIR/.env"

case "$COMMAND" in
  install)
    INSTALL_MODE=true
    [[ "$RUN_ID" =~ ^[0-9]+$ ]] || die "install requires --run-id N"
    valid_port "$UDP_MIN" || die "invalid UDP minimum port: $UDP_MIN"
    valid_port "$UDP_MAX" || die "invalid UDP maximum port: $UDP_MAX"
    (( 10#$UDP_MIN <= 10#$UDP_MAX )) || die "UDP minimum exceeds maximum"
    [[ "$ADMIN_USER" =~ ^[A-Za-z0-9_.-]+$ ]] || die "admin username must contain only letters, numbers, '.', '_' or '-'"
    [[ ! -e "$COMPOSE_FILE" ]] || die "$COMPOSE_FILE already exists; use update"
    [[ ! -e "$ENV_FILE" ]] || die "$ENV_FILE already exists; use update"
    if [[ "$DRY_RUN" == true ]]; then
      [[ -n "$PUBLIC_IP" ]] || PUBLIC_IP='<public-ip>'
      valid_ipv4 "$PUBLIC_IP" 2>/dev/null || [[ "$PUBLIC_IP" == '<public-ip>' ]] || die "invalid public IPv4: $PUBLIC_IP"
      IMAGE_TAG="ghcr.io/$REPO:run-$RUN_ID"
      echo "Planned $COMPOSE_FILE (mode 644):"
      render_compose "$IMAGE_TAG" "$PUBLIC_IP" "$UDP_MIN" "$UDP_MIN" "$UDP_MAX"
      echo "Files that would be created:"
      echo "  $COMPOSE_FILE (mode 644)"
      echo "  $ENV_FILE (mode 600)"
      echo "  $PREVIOUS_FILE (mode 644)"
      exit 0
    fi
    check_install_prereqs
    resolve_public_ip
    prepare_artifact
    mkdir -p "$COMPOSE_DIR"
    [[ ! -e "$COMPOSE_FILE" && ! -e "$ENV_FILE" ]] || die "compose.yaml or .env appeared during install; refusing to overwrite"
    generate_install_secrets
    render_compose "$IMAGE_TAG" "$PUBLIC_IP" "$UDP_MIN" "$UDP_MIN" "$UDP_MAX" >"$COMPOSE_FILE"
    chmod 644 "$COMPOSE_FILE"
    cp -p "$COMPOSE_FILE" "$PREVIOUS_FILE"
    chmod 644 "$PREVIOUS_FILE"
    ARTIFACT_DIR="$TMP_DIR/artifact"
    deploy_artifact
    echo "Admin username: $ADMIN_USER"
    printf 'Admin password: '
    cat "$TMP_DIR/admin-password"
    echo "Credentials are stored in $ENV_FILE (mode 600); the password is also stored there."
    ;;
  update)
    [[ "$RUN_ID" =~ ^[0-9]+$ ]] || die "update requires --run-id N"
    valid_port "$UDP_MIN" || die "invalid UDP minimum port: $UDP_MIN"
    valid_port "$UDP_MAX" || die "invalid UDP maximum port: $UDP_MAX"
    (( 10#$UDP_MIN <= 10#$UDP_MAX )) || die "UDP minimum exceeds maximum"
    [[ -f "$COMPOSE_FILE" ]] || die "missing $COMPOSE_FILE"
    need docker; need gh; need sha256sum; need python3
    resolve_public_ip
    prepare_artifact
    if [[ "$DRY_RUN" == true ]]; then
      update_compose true
      exit 0
    fi
    if [[ ! -f "$PREVIOUS_FILE" ]]; then cp -p "$COMPOSE_FILE" "$PREVIOUS_FILE"; fi
    update_compose false
    compose config -q
    ARTIFACT_DIR="$TMP_DIR/artifact"
    deploy_artifact
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
    docker port "$CONTAINER" 2>/dev/null | grep "/udp" || true
    ;;
  uninstall)
    need docker
    compose down
    if [[ "$PURGE_DATA" == true ]]; then
      read -r -p "Delete volumes and $COMPOSE_DIR/.env? Type yes: " answer
      [[ "$answer" == yes ]] || die "purge cancelled"
      compose down -v
      rm -f -- "$ENV_FILE"
      echo "Removed service, named volumes, and .env."
    else
      echo "Removed service; named volumes and .env were kept."
    fi
    ;;
  *) die "unknown subcommand: $COMMAND (use --help)" ;;
esac
