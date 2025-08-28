# Follows from XChem Luigi Pipeline

CONFIG_FILE="config_xchem.json"

. /etc/profile.d/modules.sh
module load uv

########################################################################
# Get the runtime location of this bash script
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
########################################################################

SCRIPT_DIR="$(realpath "$DIR/..")"

# Separate list1 and list2 at the '--' delimiter
args1=()
args2=()
mode=1

for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then
    mode=2
    continue
  fi

  if [[ $mode -eq 1 ]]; then
    args1+=("$arg")
  else
    args2+=("$arg")
  fi
done

set -x

export ZOCALO_CONFIG="/dls_sw/apps/zocalo/live/configuration.yaml"

uv run --with=zocalo --script ${DIR}/formulatrix_uploader_xchem.py --barcode_list "${args1[@]}" --proposal_list "${args2[@]}" \
  --config "${DIR}/${CONFIG_FILE}"
