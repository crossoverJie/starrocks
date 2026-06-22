#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <tag>"
    echo "Example: $0 3.5.17-v1"
    exit 1
fi

TAG="$1"

# Check tracked files have no uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: tracked files have uncommitted changes. Please commit or stash first."
    exit 1
fi

# Create and push tag
echo "Creating tag: ${TAG}"
git tag -a "${TAG}" -m "Release ${TAG}"

echo "Pushing tag to remote..."
git push origin "${TAG}"

echo "Done! Tag ${TAG} pushed. GitLab CI will build FE and push the image:"
if echo "${TAG}" | grep -q '^bigdata'; then
    echo "  harbor-tx.ypsx-internal.com/bigdata/starrocks/fe-ubuntu:${TAG}"
else
    echo "  harbor-tx.ypsx-internal.com/chenjie/fe-ubuntu:${TAG}"
fi
