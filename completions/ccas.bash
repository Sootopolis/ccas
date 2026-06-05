# bash completion for ccas — static, JVM-free (instant), see Sootopolis/ccas#49.
#
# The dynamic zio-cli completion calls back into the binary on every <TAB>, booting a JVM (~1.5s).
# This script is hand-maintained to mirror the ccas.cli.CliCommand tree. Keep it in sync:
# TestCcasCompletion asserts every command and option in the tree appears below.
#
# Install:  cp completions/ccas.bash ~/.local/share/bash-completion/completions/ccas
# (bash-completion auto-loads it; or `source` it from ~/.bashrc)

_ccas() {
  local cur prev words cword
  if declare -F _init_completion >/dev/null 2>&1; then
    _init_completion -n : || return
  else
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    cword=$COMP_CWORD
    words=("${COMP_WORDS[@]}")
  fi

  local top="serve membership history recruit stats jobs logs blacklist schedule"
  local global="--help --version"

  # First non-flag word after "ccas" is the command; the next is the subcommand (for blacklist/schedule).
  local cmd="" sub="" i
  for (( i = 1; i < cword; i++ )); do
    case "${words[i]}" in
      -*) ;;
      *)
        if [[ -z $cmd ]]; then cmd="${words[i]}"; else sub="${words[i]}"; break; fi
        ;;
    esac
  done

  local opts=""
  case "$cmd" in
    "")
      COMPREPLY=( $(compgen -W "$top $global" -- "$cur") ); return ;;
    serve)      opts="--server" ;;
    membership) opts="--server --trust-usernames --no-trust-usernames" ;;
    history)    opts="--server --full --include-finished --refresh --refresh-min-hours" ;;
    recruit)    opts="--server --alias --target --cumulative --source-clubs --time-limit-minutes --explore --no-explore" ;;
    stats)      opts="--server --since --until" ;;
    jobs)       opts="--server --limit" ;;
    logs)       opts="--server" ;;
    blacklist)
      case "$sub" in
        "")     COMPREPLY=( $(compgen -W "add list remove --help" -- "$cur") ); return ;;
        add)    opts="--server --reason --months" ;;
        list)   opts="--server" ;;
        remove) opts="--server" ;;
        *)      COMPREPLY=(); return ;;
      esac ;;
    schedule)
      case "$sub" in
        "")     COMPREPLY=( $(compgen -W "list add remove --help" -- "$cur") ); return ;;
        list)   opts="--server" ;;
        add)    opts="--server --kind --interval-hours --club --params" ;;
        remove) opts="--server" ;;
        *)      COMPREPLY=(); return ;;
      esac ;;
    *)
      COMPREPLY=(); return ;;
  esac

  COMPREPLY=( $(compgen -W "$opts --help" -- "$cur") )
}
complete -F _ccas ccas
