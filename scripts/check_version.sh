#!/usr/bin/env bash
set -euo pipefail

cargo_version="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')"
file_version="$(cat VERSION)"

if [[ "$cargo_version" != "$file_version" ]]; then
  echo "❌ Version mismatch detected"
  echo "   Cargo.toml version: $cargo_version"
  echo "   VERSION file:       $file_version"
  echo
  echo "Fix by running:"
  echo "   echo \"$cargo_version\" > VERSION"
  exit 1
fi

echo "✅ Version check passed ($cargo_version)"
