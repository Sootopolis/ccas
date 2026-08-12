#!/usr/bin/env bash
#
# One-shot local install of the `ccas` CLI on a dev machine: stage the launcher,
# put it on PATH, and wire up shell completion.
#
# Every step is idempotent — re-run it after a `git pull` (with --stage) and it
# restages, refreshes the completion script, and leaves the shell rc file alone
# when the source line is already there.
#
# What this does NOT do: server configuration. The staged binary reads the
# process environment and ~/.config/ccas/ccas.env — never the repo .env (that is
# auto-sourced only for `sbt run`). Run `ccas config init` afterwards to write
# the contact email and DB connection.
#
# Usage:
#   scripts/install-cli.sh [--shell zsh|bash|fish|none] [--bin-dir DIR]
#                          [--stage] [--no-stage] [--no-rc]
#
#   --shell     which shell to install completion for (default: $SHELL, else none)
#   --bin-dir   where the launcher symlink goes    (default: $CCAS_BIN_DIR, else ~/.local/bin)
#   --stage     force `sbt stage` even if a launcher already exists
#   --no-stage  never stage; fail if no launcher exists yet
#   --no-rc     write the completion script but do not touch any rc file
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$REPO_ROOT/target/universal/stage/bin/ccas"
BIN_DIR="${CCAS_BIN_DIR:-$HOME/.local/bin}"
XDG_CONFIG="${XDG_CONFIG_HOME:-$HOME/.config}"
XDG_DATA="${XDG_DATA_HOME:-$HOME/.local/share}"

SHELL_NAME=""
STAGE_MODE="auto"   # auto | always | never
TOUCH_RC=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --shell)    SHELL_NAME="${2:?--shell needs a value}"; shift 2 ;;
    --bin-dir)  BIN_DIR="${2:?--bin-dir needs a value}"; shift 2 ;;
    --stage)    STAGE_MODE="always"; shift ;;
    --no-stage) STAGE_MODE="never"; shift ;;
    --no-rc)    TOUCH_RC=0; shift ;;
    -h|--help)  sed -n '3,23p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)          echo "unknown argument: $1 (try --help)" >&2; exit 2 ;;
  esac
done

if [[ -z "$SHELL_NAME" ]]; then
  SHELL_NAME="$(basename "${SHELL:-}")"
fi
case "$SHELL_NAME" in
  zsh|bash|fish|none) ;;
  *) echo "note: unrecognised shell '${SHELL_NAME:-<unset>}' — skipping completion (use --shell)"
     SHELL_NAME="none" ;;
esac

# --- 1. stage the launcher ---------------------------------------------------

case "$STAGE_MODE" in
  always) do_stage=1 ;;
  never)  do_stage=0 ;;
  auto)   [[ -x "$LAUNCHER" ]] && do_stage=0 || do_stage=1 ;;
esac

if [[ $do_stage -eq 1 ]]; then
  command -v sbt >/dev/null || { echo "sbt not found on PATH — install it, or pass --no-stage" >&2; exit 1; }
  echo "staging (sbt stage)…"
  (cd "$REPO_ROOT" && sbt -batch stage)
fi

[[ -x "$LAUNCHER" ]] || { echo "no launcher at $LAUNCHER — run without --no-stage" >&2; exit 1; }

# --- 2. put it on PATH -------------------------------------------------------

mkdir -p "$BIN_DIR"
# -n so an existing symlink is replaced rather than followed into a directory.
ln -sfn "$LAUNCHER" "$BIN_DIR/ccas"
echo "linked $BIN_DIR/ccas -> $LAUNCHER"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "warning: $BIN_DIR is not on your PATH; add:  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# --- 3. shell completion -----------------------------------------------------

# Write generated output temp-then-rename so an interrupted run never leaves a
# half-written completion script that the shell would source on next login.
generate() {  # generate <shell> <dest>
  local sh="$1" dest="$2"
  mkdir -p "$(dirname "$dest")"
  "$LAUNCHER" completion "$sh" > "$dest.tmp"
  mv "$dest.tmp" "$dest"
  echo "wrote $dest"
}

# Append a source line to an rc file once, tagged so a re-run can find it.
MARKER="# added by ccas scripts/install-cli.sh"
ensure_rc_line() {  # ensure_rc_line <rc-file> <line>
  local rc="$1" line="$2"
  if [[ $TOUCH_RC -eq 0 ]]; then
    echo "add this to $rc yourself (--no-rc):  $line"
    return
  fi
  if [[ -f "$rc" ]] && grep -Fqs "$line" "$rc"; then
    echo "$rc already sources the completion script"
    return
  fi
  printf '\n%s\n%s\n' "$MARKER" "$line" >> "$rc"
  echo "appended completion source line to $rc"
}

case "$SHELL_NAME" in
  zsh)
    comp="$XDG_CONFIG/ccas/completion.zsh"
    generate zsh "$comp"
    # The script ends in `compdef _ccas ccas`, so it must be sourced *after*
    # compinit — appending at the end of .zshrc satisfies that in a normal rc.
    # Sourcing a pre-generated file (rather than the documented
    # `eval "$(ccas completion zsh)"`) keeps shell startup JVM-free.
    ensure_rc_line "$HOME/.zshrc" "source $comp"
    echo "reload with:  exec zsh"
    ;;
  bash)
    comp="$XDG_DATA/bash-completion/completions/ccas"
    generate bash "$comp"
    # bash-completion autoloads by command name from that directory, so no rc
    # edit is needed when it is installed. Without it, source the file directly.
    if [[ -r /usr/share/bash-completion/bash_completion || -r /opt/homebrew/etc/profile.d/bash_completion.sh || -r /usr/local/etc/profile.d/bash_completion.sh ]]; then
      echo "bash-completion detected — it autoloads $comp, no rc edit needed"
    else
      ensure_rc_line "$HOME/.bashrc" "source $comp"
    fi
    echo "reload with:  exec bash"
    ;;
  fish)
    comp="$XDG_CONFIG/fish/completions/ccas.fish"
    generate fish "$comp"
    echo "fish autoloads $comp, no rc edit needed"
    ;;
  none)
    echo "skipped completion"
    ;;
esac

# --- 4. what is left to do by hand -------------------------------------------

cat <<EOF

next:
  ccas config init          # contact email + DB connection -> $XDG_CONFIG/ccas/ccas.env (0600)
  ccas server up --detach   # start the server, then: ccas server status
  ccas use-club <slug>      # default club for this machine
EOF
