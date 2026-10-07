#!/usr/bin/env bash
# Pinned Linux x86_64 clients, installed without sudo or user credential access.
set -euo pipefail
if [[ "$(uname -s)/$(uname -m)" != Linux/x86_64 ]]; then
  echo 'This installer supports Linux x86_64; install equivalent clients on other platforms.' >&2
  exit 1
fi
for tool in curl unzip sha256sum go; do
  command -v "$tool" >/dev/null || { echo "Required tool missing: $tool" >&2; exit 1; }
done
prefix="${1:-${HOME}/.local/share/lamina-e2e-clients}"
mkdir -p "$prefix"
prefix="$(cd "$prefix" && pwd)"
work="$(mktemp -d /tmp/lamina-e2e-install-XXXXXXXX)"
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$prefix/bin"
curl --fail --location --silent --show-error \
  https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.34.7.zip -o "$work/aws.zip"
echo "d6b6e2291456704a441e970bbdb69466629510dd0b578e8812f7856ac64abba1  $work/aws.zip" | sha256sum --check --status
unzip -q "$work/aws.zip" -d "$work"
"$work/aws/install" --install-dir "$prefix/aws-cli" --bin-dir "$prefix/bin" --update
curl --fail --location --silent --show-error \
  https://downloads.rclone.org/v1.73.1/rclone-v1.73.1-linux-amd64.zip -o "$work/rclone.zip"
echo "e9bad0be2ed85128e0d977bf36c165dd474a705ea950d18e1005cef98119407b  $work/rclone.zip" | sha256sum --check --status
unzip -q "$work/rclone.zip" -d "$work"
install -m 755 "$work/rclone-v1.73.1-linux-amd64/rclone" "$prefix/bin/rclone"
# The upstream binary archive returns HTTP 410. Build the exact release commit;
# Go's module checksum database verifies the source/dependency downloads.
GOBIN="$prefix/bin" go install github.com/minio/mc@v0.0.0-20250813083541-7394ce0dd2a8
"$prefix/bin/aws" --version
"$prefix/bin/rclone" version
"$prefix/bin/mc" --version
go version -m "$prefix/bin/mc"
printf '\nAdd this directory to PATH: %s/bin\n' "$prefix"
