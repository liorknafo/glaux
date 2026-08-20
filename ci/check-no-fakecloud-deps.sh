#!/usr/bin/env bash
# Enforce the Apache/AGPL dependency boundary from the v0.1 design spec:
# the Apache-2.0 crates (the three engine libraries and glaux-server) must
# have zero fakecloud-* dependencies, transitively. Only the AGPL-3.0
# all-in-one `glaux` binary may link fakecloud crates.
#
# Usage: ci/check-no-fakecloud-deps.sh
set -euo pipefail

cd "$(dirname "$0")/.."

CRATES=(glaux-athena glaux-firehose glaux-catalog glaux-server)
status=0

for crate in "${CRATES[@]}"; do
    # --edges normal,build: runtime and build-dependencies count; dev-deps of
    # transitive crates are never linked so they are excluded by default.
    tree="$(cargo tree --package "$crate" --edges normal,build --prefix none)"
    offenders="$(grep -E '^fakecloud(-[A-Za-z0-9_-]+)? ' <<<"$tree" || true)"
    if [[ -n "$offenders" ]]; then
        echo "ERROR: $crate transitively depends on fakecloud crates:" >&2
        echo "$offenders" >&2
        status=1
    else
        echo "OK: $crate has zero fakecloud-* dependencies"
    fi
done

exit $status
