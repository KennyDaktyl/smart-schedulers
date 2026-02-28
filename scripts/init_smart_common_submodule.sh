#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

if [ ! -f ".gitmodules" ]; then
  cat > .gitmodules <<'EOF'
[submodule "smart_common"]
	path = smart_common
	url = git@github.com:KennyDaktyl/smart_common.git
	branch = develop
EOF
fi

if [ ! -d ".git/modules/smart_common" ] && [ -d "smart_common/.git" ]; then
  rm -rf smart_common
fi

if ! git config -f .gitmodules --get submodule.smart_common.path >/dev/null 2>&1; then
  git submodule add -f -b develop git@github.com:KennyDaktyl/smart_common.git smart_common
fi

git config -f .gitmodules submodule.smart_common.path smart_common
git config -f .gitmodules submodule.smart_common.url git@github.com:KennyDaktyl/smart_common.git
git config -f .gitmodules submodule.smart_common.branch develop

git submodule sync --recursive
git submodule update --init --recursive
git submodule update --remote smart_common

echo "Submodule ready: smart_common"
