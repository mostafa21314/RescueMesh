#!/usr/bin/env bash
# Build release binaries for every target this machine can build, and put each in
# dist/<target>/ with config/, scripts/ and DEPLOYMENT.md, ready to copy to a USB
# stick.
#
#   scripts/package.sh
#
# Targets tried: this machine's own (always), x86_64-pc-windows-gnu and
# x86_64-unknown-linux-musl. A cross target is built only if its Rust standard
# library is installed (rustup target add <target>) and its linker is on PATH;
# otherwise it is skipped with what is missing. See docs/deployment.md, "Release
# builds".
set -uo pipefail
cd "$(dirname "$0")/.."

host=$(rustc -vV | sed -n 's/^host: //p')
sysroot=$(rustc --print sysroot)
built=()
skipped=()

# has_std TARGET: the target's standard library is installed (rustup or not).
has_std() { [[ -d "$sysroot/lib/rustlib/$1/lib" ]]; }

# stage TARGET DIR EXE_SUFFIX: copy binaries and files into dist/TARGET.
stage() {
  local target=$1 from=$2 ext=$3 out="dist/$1"
  rm -rf "$out"
  mkdir -p "$out"
  cp "$from/cloud-node$ext" "$from/client$ext" "$from/stress$ext" "$out/"
  cp -R config scripts "$out/"
  cp docs/deployment.md "$out/DEPLOYMENT.md"
  {
    echo "target   $target"
    echo "commit   $(git rev-parse --short HEAD 2>/dev/null || echo unknown)$(git diff --quiet 2>/dev/null || echo ' (with uncommitted changes)')"
    echo "rustc    $(rustc --version)"
    echo "built on $(uname -sm), $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$out/BUILD.txt"
  built+=("$out")
}

# build TARGET LINKER_ENV LINKER: cross-build TARGET with LINKER.
build() {
  local target=$1 env_name=$2 linker=$3 ext=""
  [[ $target == *windows* ]] && ext=".exe"
  echo "== $target"
  if ! has_std "$target"; then
    skipped+=("$target: Rust std for it not installed (needs rustup: rustup target add $target)")
    echo "   skipped"
    return
  fi
  if ! command -v "$linker" >/dev/null; then
    skipped+=("$target: linker $linker not on PATH")
    echo "   skipped"
    return
  fi
  if env "$env_name=$linker" cargo build --release --target "$target"; then
    stage "$target" "target/$target/release" "$ext"
  else
    skipped+=("$target: cargo build failed (see output above)")
  fi
}

echo "== $host (this machine)"
if cargo build --release; then
  ext=""
  [[ $host == *windows* ]] && ext=".exe"
  stage "$host" target/release "$ext"
else
  skipped+=("$host: cargo build failed (see output above)")
fi

[[ $host != x86_64-pc-windows-gnu ]] &&
  build x86_64-pc-windows-gnu CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER x86_64-w64-mingw32-gcc

if [[ $host == x86_64-unknown-linux-gnu ]]; then
  build x86_64-unknown-linux-musl CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER cc
else
  build x86_64-unknown-linux-musl CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER x86_64-linux-musl-gcc
fi

echo
echo "built:"
for d in ${built[@]+"${built[@]}"}; do echo "  $d/"; done
[[ ${#built[@]} -gt 0 ]] || echo "  nothing"
if [[ ${#skipped[@]} -gt 0 ]]; then
  echo "skipped:"
  for s in "${skipped[@]}"; do echo "  $s"; done
fi
[[ ${#built[@]} -gt 0 ]]
