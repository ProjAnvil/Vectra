#!/usr/bin/env bash
# go get tag smoke: prove that the released tag trio (vX.Y.Z, server/vX.Y.Z,
# mcp/vX.Y.Z) cut on the version-prep commit is exactly what an outside
# consumer needs to `go get` all three Vectra modules.
#
# Why this smoke exists: server/go.mod and mcp/go.mod pin the engine with
# `require github.com/projanvil/vectra v0.0.0` + `replace ... => ../` so
# in-repo builds use the working tree. A replace directive in a dependency
# module is IGNORED when that module is consumed from outside (only the
# main module's replaces apply), so a user running
# `go get github.com/projanvil/vectra/server@vX.Y.Z` has to resolve the
# engine requirement `github.com/projanvil/vectra v0.0.0` — a version that
# has no tag and therefore fails. docs/RELEASING.md fixes this with a
# version-prep commit that bumps both require lines to the real version,
# then cuts the tag trio. This script simulates that whole flow against a
# throwaway local mirror and consumes the result like a user would.
#
# Usage:
#   scripts/goget-smoke.sh [VERSION]   # default: minor bump of the top
#                                      # CHANGELOG version (0.14.0 -> 0.15.0)
#   make goget-smoke [VERSION=0.15.0]
#
# Safety: everything lives under one mktemp -d removed on exit; no global
# git config is touched (the insteadOf rewrite is injected per git process
# via GIT_CONFIG_COUNT / GIT_CONFIG_KEY_0 / GIT_CONFIG_VALUE_0 env vars);
# third-party dependencies still resolve through the default module proxy —
# only github.com/projanvil/* goes direct (GOPRIVATE) to the local mirror.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)

# Hermetic identity for the smoke's commits and annotated tags
# (per-command -c flags; global git config is never read or changed).
GITX="git -c user.name=vectra-smoke -c user.email=smoke@projanvil.invalid"

step=0
say_step() { step=$1; printf 'STEP %d: %s\n' "$1" "$2"; }
say_pass() { printf 'STEP %s PASS: %s\n' "$step" "$1"; }
say_fail() { printf 'STEP %s FAIL: %s\n' "$step" "$1" >&2; exit 1; }

# --- version: argument, else minor bump of the top CHANGELOG version ----
if [ $# -ge 1 ]; then
    ver=$1
else
    base=$(sed -n 's/^## \[\([0-9][0-9]*\)\.\([0-9][0-9]*\)\.[0-9][0-9]*\].*/\1 \2/p' \
        "$root/CHANGELOG.md" | head -n 1)
    [ -n "$base" ] || { echo "FAIL: no '## [x.y.z]' heading found in CHANGELOG.md" >&2; exit 1; }
    ver="${base% *}.$(( ${base#* } + 1 )).0"
fi
case $ver in
    v*) echo "FAIL: pass a bare version (e.g. 0.15.0), not '$ver'" >&2; exit 1 ;;
    *.*.*) ;;
    *) echo "FAIL: '$ver' is not an x.y.z version" >&2; exit 1 ;;
esac

tmp=$(mktemp -d "${TMPDIR:-/tmp}/vectra-goget-smoke.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

printf 'go get tag smoke: releasing v%s into the throwaway mirror %s\n' "$ver" "$tmp"

# --- Step 1: work mirror (a clone this script may commit and tag in) -----
say_step 1 "clone the repository into a throwaway work mirror"
git clone -q "$root" "$tmp/work" || say_fail "git clone $root"
cd "$tmp/work"
say_pass "cloned $(git log --oneline -1)"

# --- Step 2: version-prep commit + annotated tag trio --------------------
say_step 2 "version-prep commit (require vectra v$ver) + tags v$ver server/v$ver mcp/v$ver"
for m in server mcp; do
    [ "$(grep -c 'github.com/projanvil/vectra v0\.0\.0' "$m/go.mod")" -eq 1 ] \
        || say_fail "$m/go.mod: expected exactly one 'github.com/projanvil/vectra v0.0.0' require line"
    sed -i.bak "s|github.com/projanvil/vectra v0\.0\.0|github.com/projanvil/vectra v$ver|" "$m/go.mod"
    rm -f "$m/go.mod.bak"
    grep -q "github.com/projanvil/vectra v$ver" "$m/go.mod" \
        || say_fail "$m/go.mod: require line was not bumped to v$ver"
    grep -q '^replace github.com/projanvil/vectra => \.\./$' "$m/go.mod" \
        || say_fail "$m/go.mod: the replace directive must survive the version prep"
done
$GITX add server/go.mod mcp/go.mod || say_fail "git add of the bumped go.mod files"
$GITX commit -q -m "chore(release): vectra v$ver (go get tag smoke)" \
    || say_fail "version-prep commit"
for t in "v$ver" "server/v$ver" "mcp/v$ver"; do
    $GITX tag -a "$t" -m "vectra $t (go get tag smoke)" || say_fail "annotated tag $t"
done
say_pass "committed and tagged $(git rev-parse --short HEAD): v$ver server/v$ver mcp/v$ver"

# --- Step 3: bare mirror (what insteadOf will point at) ------------------
say_step 3 "publish the work mirror as a bare repository"
git clone -q --bare "$tmp/work" "$tmp/mirror.git" || say_fail "bare clone into $tmp/mirror.git"
for t in "v$ver" "server/v$ver" "mcp/v$ver"; do
    git --git-dir="$tmp/mirror.git" rev-parse -q --verify "refs/tags/$t" >/dev/null \
        || say_fail "tag $t missing from the bare mirror"
done
say_pass "bare mirror carries all three tags"

# --- Step 4: scratch consumer module --------------------------------------
say_step 4 "create the scratch consumer (one real root API call + blank server/mcp imports)"
mkdir "$tmp/smoke" || say_fail "mkdir $tmp/smoke"
cd "$tmp/smoke"
go mod init smoke.local/smoke || say_fail "go mod init"
cat > main.go <<'EOF'
// Smoke consumer for the Vectra release tags: the root import makes one
// real engine call (Open a graph in a temp dir, one quickstart write,
// Close), and the blank imports force the server and mcp modules into the
// build so all three tags must resolve for `go build ./...` to pass.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/projanvil/vectra"
	_ "github.com/projanvil/vectra/mcp"
	_ "github.com/projanvil/vectra/server"
)

func main() {
	dir, err := os.MkdirTemp("", "vectra-goget-smoke-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	g, err := vectra.Open(filepath.Join(dir, "smoke.vg"), vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		panic(err)
	}
	g.AddNode("gateway", "Service", map[string]any{"env": "prod"})
	if err := g.Close(); err != nil {
		panic(err)
	}
	fmt.Println("smoke consumer: engine open/write/close ok")
}
EOF
say_pass "scratch module smoke.local/smoke with main.go"

# --- Step 5: resolution environment ---------------------------------------
say_step 5 "point github.com/projanvil/* at the local mirror (GOPRIVATE + insteadOf via env)"
export GOPRIVATE='github.com/projanvil/*'
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0="url.$tmp/mirror.git.insteadOf"
export GIT_CONFIG_VALUE_0='https://github.com/projanvil/vectra'
# Prove the rewrite reaches the mirror before go touches it: this URL is
# rewritten to the local bare repo, so ls-remote must answer offline.
git ls-remote "https://github.com/projanvil/vectra" >/dev/null \
    || say_fail "insteadOf rewrite does not reach $tmp/mirror.git"
say_pass "github.com/projanvil/vectra resolves to the local mirror"

# --- Step 6: go get the trio and build ------------------------------------
say_step 6 "go get vectra@v$ver, server@v$ver, mcp@v$ver then go build ./..."
go get "github.com/projanvil/vectra@v$ver"        || say_fail "go get github.com/projanvil/vectra@v$ver"
go get "github.com/projanvil/vectra/server@v$ver" || say_fail "go get github.com/projanvil/vectra/server@v$ver"
go get "github.com/projanvil/vectra/mcp@v$ver"    || say_fail "go get github.com/projanvil/vectra/mcp@v$ver"
grep -q "github.com/projanvil/vectra v$ver" go.mod        || say_fail "go.mod does not require vectra v$ver"
grep -q "github.com/projanvil/vectra/server v$ver" go.mod || say_fail "go.mod does not require server v$ver"
grep -q "github.com/projanvil/vectra/mcp v$ver" go.mod    || say_fail "go.mod does not require mcp v$ver"
go build ./... || say_fail "go build ./... against the tagged modules"
say_pass "all three tags resolve and the consumer builds"

printf 'ALL STEPS PASS: go get of v%s / server/v%s / mcp/v%s works from a clean consumer\n' \
    "$ver" "$ver" "$ver"
