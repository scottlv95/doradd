#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] [-f] -p param_value arg1 [arg2...]

Script description here.

Available options:

-h, --help      Print this help and exit
-v, --verbose   Print script debug info
-f, --flag      Some flag description
-p, --param     Some param description
EOF
  exit
}

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

setup_colors() {
  if [[ -t 2 ]] && [[ -z "${NO_COLOR-}" ]] && [[ "${TERM-}" != "dumb" ]]; then
    NOFORMAT='\033[0m' RED='\033[0;31m' GREEN='\033[0;32m' ORANGE='\033[0;33m' BLUE='\033[0;34m' PURPLE='\033[0;35m' CYAN='\033[0;36m' YELLOW='\033[1;33m'
  else
    NOFORMAT='' RED='' GREEN='' ORANGE='' BLUE='' PURPLE='' CYAN='' YELLOW=''
  fi
}

msg() {
  echo >&2 -e "${1-}"
}

die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  msg "$msg"
  exit "$code"
}

parse_params() {
  # default values of variables set from params
  # TODO: mod flag as core_count
  flag=0
  type=''

  while :; do
    case "${1-}" in
    -h | --help) usage ;;
    -v | --verbose) set -x ;;
    --no-color) NO_COLOR=1 ;;
    -f | --flag) flag=1 ;; # example flag
    -c | --type)
      type="${2-}"
      shift
      ;;
    -?*) die "Unknown option: $1" ;;
    *) break ;;
    esac
    shift
  done

  args=("$@")

  # check required params and arguments
  [[ -z "${type-}" ]] && die "Missing required parameter: type"
  case "$type" in
    "mixed" | "nft" | "p2p" | "dex_avg" | "dex_bur")
        # echo "type: $type"
        ;;
    *)
        echo "Error: Invalid parameter."
        exit 1
        ;;
  esac

  # [[ ${#args[@]} -eq 0 ]] && die "Missing script arguments"

  return 0
}

parse_params "$@"
setup_colors

# script logic here

check_pattern() {
  # Check if the line matches the expected pattern
  # $1: line_number, $2: file, $3: pattern
  if ! sed -n "$1p" "$2" | grep -qF "$3"; then
    msg "Error: Line $1 in $2 does not match split macro."
    exit 1
  fi
}

set_workload_type() {
  local file="${script_dir}/../../src/bench/chain.cc" 
  local pattern="using TxnType = *"
  local line_number=352
  check_pattern $line_number $file $pattern

  sed -i "${line_number}s/=.*/= $1;/" "$file"
}

workload_setup() {

  case "$type" in
    "mixed" )
      msg "mixed type"
      set_workload_type Mixed
      ;;
    "nft" )
      msg "nft type"
      set_workload_type Nft 
      ;;
    "p2p" )
      msg "p2p type"
      set_workload_type P2p
      ;;
    "dex_avg" )
      msg "dex type (avg)"
      set_workload_type Dex
      ;;
    "dex_bur" )
      msg "dex type (bur)"
      set_workload_type Dex 
      ;;
 esac
  cd ${script_dir}/../../src/bench/build && ninja
  cd ${script_dir}
}

workload_setup
