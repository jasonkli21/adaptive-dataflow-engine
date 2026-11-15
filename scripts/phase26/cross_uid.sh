#!/usr/bin/env bash
# Run only inside a disposable Linux environment with noninteractive sudo.
set -euo pipefail
bin=$(realpath "$1")
test "$(uname -s)" = Linux
test "$(id -u)" -ne 0
test -x "$bin"
work=$(mktemp -d /tmp/ade26-owner.XXXXXX)
other=$(sudo -n -u nobody mktemp -d /tmp/ade26-other.XXXXXX)
trap 'rm -rf -- "$work"; sudo -n -u nobody rm -rf -- "$other"' EXIT
cp "$bin" "$work/ade-cache"
chmod 755 "$work" "$work/ade-cache"
bin="$work/ade-cache"

owner_uid=$(id -u)
other_uid=$(sudo -n -u nobody id -u)
test "$owner_uid" -ne "$other_uid"
printf 'owner_uid=%s\nother_uid=%s\n' "$owner_uid" "$other_uid"
sha256sum "$bin"

mkdir -m 700 "$work/cache"
"$bin" init --cache "$work/cache"
"$bin" inspect --cache "$work/cache"
sudo -n -u nobody mkdir -m 700 "$other/cache"
sudo -n -u nobody "$bin" init --cache "$other/cache"
sudo -n -u nobody "$bin" inspect --cache "$other/cache"

expect_refusal() {
    label=$1
    shift
    code=0
    "$@" >"$work/out" 2>"$work/err" || code=$?
    test "$code" = 1
    test ! -s "$work/out"
    grep -qx '{"action":"refuse","error":"access refused","version":1}' "$work/err"
    printf '%s exit=%s %s\n' "$label" "$code" "$(cat "$work/err")"
}
expect_refusal other_inspect_owner sudo -n -u nobody "$bin" inspect --cache "$work/cache"
expect_refusal other_gc_owner sudo -n -u nobody "$bin" gc --cache "$work/cache"
expect_refusal owner_inspect_other "$bin" inspect --cache "$other/cache"
expect_refusal owner_gc_other "$bin" gc --cache "$other/cache"
if sudo -n -u nobody cat "$work/cache/root.json" >"$work/out" 2>"$work/err"; then
    exit 1
fi
test ! -s "$work/out"
printf 'other_raw_metadata_denied=true\n'
