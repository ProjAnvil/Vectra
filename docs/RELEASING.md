# Releasing Vectra

Vectra is one repository publishing three Go modules. A release is one
commit — the version-prep commit — carrying all three annotated tags:

| Module | Tag | Ships |
| ------ | --- | ----- |
| `github.com/projanvil/vectra` | `vX.Y.Z` | the embedded engine (zero dependencies) |
| `github.com/projanvil/vectra/server` | `server/vX.Y.Z` | raft/HTTP server + `vectra-server`, `vectra-bench`, `vectra-cli` |
| `github.com/projanvil/vectra/mcp` | `mcp/vX.Y.Z` | MCP agent gateway (`vectra-mcp`) |

`server/go.mod` and `mcp/go.mod` both carry
`require github.com/projanvil/vectra v0.0.0` with
`replace github.com/projanvil/vectra => ../` so in-repo (workspace) builds
compile against the engine working tree. The release flow below turns that
placeholder into the real version for the tagged commit — see
[go get compatibility](#go-get-compatibility) for why this is not optional.

Historical note: releases up to `v0.13.1` tagged only the root module; the
`server/` and `mcp/` submodule tags start with the first release that
follows this guide (the `server` and `mcp` modules were not independently
`go get`-able before then).

## Release checklist

Replace `X.Y.Z` with the release version (e.g. `0.15.0`) throughout.

1. **Date the changelog.** In `CHANGELOG.md`, turn the top
   `## [X.Y.Z] - Unreleased` heading into `## [X.Y.Z] - YYYY-MM-DD`.

2. **Version-prep commit.** Bump the engine require line in both dependency
   modules from the placeholder to the release version; keep the `replace`
   directives (they are ignored by consumers and keep in-repo builds on the
   working tree). The guard between the sed and the commit is mandatory:
   `sed` exits 0 even when its pattern matched nothing (state drift, an
   interrupted release, a future go.mod refactor), and committing a missed
   bump under the tag trio ships exactly the broken release this checklist
   exists to prevent:

   ```sh
   sed -i.bak 's|github.com/projanvil/vectra v0\.0\.0|github.com/projanvil/vectra vX.Y.Z|' server/go.mod mcp/go.mod
   rm -f server/go.mod.bak mcp/go.mod.bak
   for m in server mcp; do
       grep -q "github.com/projanvil/vectra vX.Y.Z" "$m/go.mod" \
           && grep -q '^replace github.com/projanvil/vectra => \.\./$' "$m/go.mod" \
           || { echo "ERROR: $m/go.mod is not version-prepped for vX.Y.Z — do NOT tag" >&2; exit 1; }
   done
   git add CHANGELOG.md server/go.mod mcp/go.mod
   git commit -m "chore(release): vectra vX.Y.Z"
   ```

   (`sed -i.bak` plus removing the `*.bak` files works on both BSD and GNU
   sed. The guard asserts the end state of both go.mod files — the bumped
   require line and the surviving `replace ../` — mirroring the checks
   `scripts/goget-smoke.sh` applies in its step 2; asserting the end state
   rather than the edit also makes re-running step 2 after an interrupted
   attempt safe.)

3. **Cut the annotated tag trio** — all three on that same commit:

   ```sh
   git tag -a vX.Y.Z        -m "vectra vX.Y.Z"
   git tag -a server/vX.Y.Z -m "vectra server vX.Y.Z"
   git tag -a mcp/vX.Y.Z    -m "vectra mcp vX.Y.Z"
   ```

   The submodule tags (`server/…`, `mcp/…`) are what `go get` resolves for
   the nested modules; the root tag covers the engine and is the GitHub
   release point.

4. **Push** the branch and the tags:

   ```sh
   git push origin main --follow-tags
   ```

   (`--follow-tags` pushes annotated tags reachable from the pushed
   commits, which the trio is. Or push the tags explicitly:
   `git push origin vX.Y.Z server/vX.Y.Z mcp/vX.Y.Z`.)

5. **Create the GitHub release.** `gh` must be pointed at the exact tagged
   commit — pass the **full SHA** (`gh` otherwise targets the current
   branch head, which may have moved past the tags, and a moving target
   breaks the submodule tag resolution the release is supposed to publish):

   ```sh
   sha=$(git rev-parse vX.Y.Z)   # full 40-char SHA of the tagged commit
   gh release create vX.Y.Z --target "$sha" \
       --title "vectra vX.Y.Z" \
       --notes "<paste the X.Y.Z section from CHANGELOG.md>"
   ```

   One release page per version is enough: the `server/` and `mcp/` tags
   exist for the Go module system, not for separate release objects.

6. **Build and publish the Docker image.** Run from the repository root —
   the build context must include the root module because `server/go.mod`
   replaces the engine with `../`:

   ```sh
   docker build -t ghcr.io/projanvil/vectra-server:vX.Y.Z \
               -t ghcr.io/projanvil/vectra-server:latest .
   docker push ghcr.io/projanvil/vectra-server:vX.Y.Z
   docker push ghcr.io/projanvil/vectra-server:latest
   ```

7. **Publish the SDKs.** Bump each package's version to `X.Y.Z` in its
   manifest first, then publish from its directory:

   - `sdk/ts` → npm `@projanvil/vectra`:
     `cd sdk/ts && npm install && npm run build && npm publish --access public`
   - `sdk/python` → PyPI `vectra-graph`:
     `cd sdk/python && python -m build && twine upload dist/*`
   - `mcp/ts` → npm `@projanvil/vectra-mcp`:
     `cd mcp/ts && npm install && npm run build && npm publish --access public`
   - `mcp/python` → PyPI `vectra-mcp-python`:
     `cd mcp/python && python -m build && twine upload dist/*`

## go get compatibility

The one rule this repository's tag strategy turns on: **a `replace`
directive in a dependency module is ignored; only the main module's
replaces apply.**

`server/go.mod` and `mcp/go.mod` carry

```
require github.com/projanvil/vectra v0.0.0

replace github.com/projanvil/vectra => ../
```

so builds inside this repository use the engine working tree. But when a
user's module requires `github.com/projanvil/vectra/server`, that inner
`replace` is inert: the user's `go get
github.com/projanvil/vectra/server@vX.Y.Z` must resolve the engine
requirement `github.com/projanvil/vectra v0.0.0` against real tags — and
`v0.0.0` is a placeholder that is never tagged. Without the version-prep
commit the user's build fails with exactly:

```
go: github.com/projanvil/vectra/server imports
	github.com/projanvil/vectra: reading github.com/projanvil/vectra/go.mod at revision v0.0.0: unknown revision v0.0.0
```

That is why step 2 of the checklist bumps both require lines to the real
version on the tagged commit, and why all three tags must sit on that same
commit: the root `vX.Y.Z` tag is what the bumped require line resolves
against. The `replace ../` lines stay in the released `go.mod` files — they
cost nothing downstream and keep workspace builds working.

## The go get tag smoke

`scripts/goget-smoke.sh` verifies the whole flow end to end without
touching GitHub. It clones the repository into a throwaway mirror, makes
the version-prep commit (bumping the two require lines exactly as step 2
prescribes), cuts the annotated tag trio, publishes the result as a bare
repository, and then — in a scratch consumer module whose `main.go` makes
one real engine call (`Open` a graph in a temp dir, one write, `Close`)
plus blank imports of `server` and `mcp` — runs the exact `go get` sequence
a user would, followed by `go build ./...`.

The mirror is selected per-process: `GOPRIVATE=github.com/projanvil/*`
sends the Vectra modules direct to git, and an
`url.<mirror>.insteadOf` rewrite is injected into the `go` command's git
subprocesses via `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_0` /
`GIT_CONFIG_VALUE_0` environment variables — no global git config is read
or modified. Third-party dependencies (hashicorp/raft, the MCP go-sdk,
...) still resolve through the default module proxy.

Run it before tagging a release, and after any change to the module
layout:

```sh
make goget-smoke                  # VERSION defaults to 0.15.0
make goget-smoke VERSION=0.16.0   # explicit
scripts/goget-smoke.sh 0.16.0     # without make; with no argument the
                                  # version is a minor bump of the top
                                  # CHANGELOG version (0.14.0 -> 0.15.0)
```

Every step prints PASS or FAIL and the script exits non-zero on the first
failure. All of its state (work mirror, bare mirror, tags, scratch
consumer) lives under one `mktemp -d` removed on exit — the repository
itself gains no tags and nothing is pushed.
