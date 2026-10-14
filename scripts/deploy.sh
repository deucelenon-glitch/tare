#!/usr/bin/env bash
set -euo pipefail
REPO="${1:?repo name required}"
DESC="TARE — True Agentic Resource Extraction"
gh auth status >/dev/null
git add -A
git diff --cached --quiet || git commit -m "Deploy TARE site"
gh repo create "$REPO" --public --source=. --push --description "$DESC" 2>&1 | tail -3
OWNER=$(gh api user --jq '.login')
echo "https://${OWNER}.github.io/${REPO}/"
