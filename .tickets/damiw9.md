---
id: damiw9
status: open
type: chore
effort: easy
created: 2026-11-21T02:07:42Z
---
# 5.9a Restore and review CODEOWNERS before merging into master

Step 5.9a of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

`.github/CODEOWNERS` was deleted on `mm/refactoring` (ticket geyab7) so that PRs into it do not request reviews from every code owner. Before `mm/refactoring` is merged into `master`, bring it back and review it: restore the file from `master` (`git checkout origin/master -- .github/CODEOWNERS`), check that every listed owner is still active and has write access, and add path-specific owners for the new layout (for example `scoring/`, `ci/databricksTests/`) if wanted. Do this as the last PR into `mm/refactoring`, otherwise later PRs request reviews again. Verify: the merge PR into `master` does not delete `.github/CODEOWNERS` (`git diff origin/master...mm/refactoring -- .github/CODEOWNERS` shows only the intended changes), and GitHub requests reviews from the expected owners.

Done = the Verify condition holds; then tk note + tk close.